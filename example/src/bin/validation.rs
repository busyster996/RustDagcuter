use async_trait::async_trait;
use rs_dagcuter::*;
use std::collections::HashMap;
use std::sync::Arc;

struct Step {
    name: &'static str,
    dependencies: Vec<String>,
    fails: bool,
}

#[async_trait]
impl Task for Step {
    fn name(&self) -> &str {
        self.name
    }

    fn dependencies(&self) -> DependencySpec {
        after(self.dependencies.iter().map(String::as_str))
    }

    async fn execute(
        &self,
        _ctx: TaskContext,
        _attempt: i64,
        input: &Input,
    ) -> (Output, Option<Error>) {
        if self.fails {
            return (
                Output::default(),
                Some(Error::TaskExecution(format!("{} failed", self.name))),
            );
        }
        (
            HashMap::from([
                ("task".into(), Arc::new(self.name) as Value),
                (
                    "received".into(),
                    Arc::new(input.outputs.keys().cloned().collect::<Vec<_>>()) as Value,
                ),
            ])
            .into(),
            None,
        )
    }
}

fn step(name: &'static str, deps: &[&str], fails: bool) -> BoxTask {
    Arc::new(Step {
        name,
        dependencies: deps.iter().map(|name| (*name).into()).collect(),
        fails,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let missing = HashMap::from([("child".into(), step("child", &["absent"], false))]);
    assert!(matches!(
        validate(&missing),
        Err(Error::UnknownDependency { .. })
    ));

    let cycle = HashMap::from([
        ("a".into(), step("a", &["b"], false)),
        ("b".into(), step("b", &["a"], false)),
    ]);
    assert!(matches!(
        validate(&cycle),
        Err(Error::CircularDependency(_))
    ));

    let mismatch = HashMap::from([("map-key".into(), step("actual-name", &[], false))]);
    assert!(validate(&mismatch).is_ok());
    assert!(matches!(
        Scheduler::new(mismatch, []),
        Err(Error::TaskNameMismatch { .. })
    ));
    println!("Validation: missing dependency, cycle, and name mismatch rejected");

    let dag = Scheduler::new(
        HashMap::from([
            ("failed".into(), step("failed", &[], true)),
            ("skipped".into(), step("skipped", &["failed"], false)),
            ("independent".into(), step("independent", &[], false)),
        ]),
        [],
    )?;
    let (outputs, error) = dag.execute(Context::new()).await;
    let error = error.expect("the failed task must return a run error");
    assert_eq!(dag.state("failed"), State::Failed);
    assert_eq!(dag.state("skipped"), State::Skipped);
    assert_eq!(dag.state("independent"), State::Success);
    assert_eq!(error.failures.len(), 1);
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        outputs["independent"]["task"]
            .downcast_ref::<&str>()
            .copied(),
        Some("independent")
    );
    println!(
        "Partial success: states={:?}, outputs={:?}",
        dag.states(),
        outputs
    );
    Ok(())
}
