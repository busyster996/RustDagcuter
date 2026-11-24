use async_trait::async_trait;
use rs_dagcuter::*;
use std::collections::HashMap;
use std::sync::Arc;

struct Step {
    name: &'static str,
    deps: DependencySpec,
    run: fn(&Input) -> (Output, Option<Error>),
}

#[async_trait]
impl Task for Step {
    fn name(&self) -> &str {
        self.name
    }

    fn dependencies(&self) -> DependencySpec {
        self.deps.clone()
    }

    async fn execute(&self, _: TaskContext, _: i64, input: &Input) -> (Output, Option<Error>) {
        (self.run)(input)
    }
}

fn step(
    name: &'static str,
    join: JoinPolicy,
    edges: &[(&str, &str)],
    run: fn(&Input) -> (Output, Option<Error>),
) -> (String, BoxTask) {
    let deps = DependencySpec {
        join,
        edges: edges
            .iter()
            .map(|&(task, on)| DependencyEdge::new(task, on))
            .collect(),
    };
    (name.into(), Arc::new(Step { name, deps, run }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use JoinPolicy::{AllOf, AnyOf};

    let tasks = HashMap::from([
        step("route", AllOf, &[], |_| {
            (
                Output {
                    data: HashMap::from([("region".into(), Arc::new("west") as Value)]),
                    emit: vec!["west".into()],
                },
                None,
            )
        }),
        step("east", AllOf, &[("route", "east")], |_| {
            (Data::new().into(), None)
        }),
        step("west", AllOf, &[("route", "west")], |input| {
            assert_eq!(
                input.outputs["route"]["region"]
                    .downcast_ref::<&str>()
                    .copied(),
                Some("west")
            );
            (
                Output::default(),
                Some(Error::TaskExecution("west service unavailable".into())),
            )
        }),
        step("recover", AllOf, &[("west", ON_FAILURE)], |input| {
            println!("Recovering from: {}", input.errors["west"]);
            (
                HashMap::from([("fallback".into(), Arc::new(true) as Value)]).into(),
                None,
            )
        }),
        step(
            "join",
            AnyOf,
            &[
                ("east", ON_SUCCESS),
                ("west", ON_SUCCESS),
                ("recover", ON_SUCCESS),
            ],
            |input| {
                assert_eq!(
                    input.outputs["recover"]["fallback"]
                        .downcast_ref::<bool>()
                        .copied(),
                    Some(true)
                );
                (Data::new().into(), None)
            },
        ),
    ]);

    let dag = Scheduler::new(tasks, [])?;
    dag.print_graph();
    let (outputs, run) = dag.execute(Context::new()).await;
    let run = run.unwrap();
    assert!(outputs.contains_key("join"));
    assert_eq!(dag.state("east"), State::Skipped);
    assert_eq!(dag.state("join"), State::Success);
    assert_eq!(dag.phase(), Phase::Failed);
    assert_eq!(run.failures[0].task, "west");
    println!("Successful completions:{}", dag.success_completion_order());
    Ok(())
}
