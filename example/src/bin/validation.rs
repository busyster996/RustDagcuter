use async_trait::async_trait;
use rs_dagcuter::{
    validate, BoxTask, Dag, Error, RetryPolicy, Task, TaskContext, TaskInput, TaskOutcome,
    TaskState,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

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

    fn dependencies(&self) -> Vec<String> {
        self.dependencies.clone()
    }

    fn retry_policy(&self) -> Option<RetryPolicy> {
        None
    }

    async fn execute(&self, _ctx: TaskContext, _attempt: u64, input: &TaskInput) -> TaskOutcome {
        if self.fails {
            return TaskOutcome::failure(Error::TaskExecution(format!("{} failed", self.name)));
        }
        TaskOutcome::success(HashMap::from([
            ("task".into(), json!(self.name)),
            ("received".into(), json!(input.keys().collect::<Vec<_>>())),
        ]))
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
        Dag::new(mismatch),
        Err(Error::TaskNameMismatch { .. })
    ));
    println!("Validation: missing dependency, cycle, and name mismatch rejected");

    let dag = Dag::new(HashMap::from([
        ("failed".into(), step("failed", &[], true)),
        ("skipped".into(), step("skipped", &["failed"], false)),
        ("independent".into(), step("independent", &[], false)),
    ]))?;
    let error = dag
        .execute(CancellationToken::new())
        .await
        .expect_err("the failed task must return a run error");
    assert_eq!(dag.state("failed"), Some(TaskState::Failed));
    assert_eq!(dag.state("skipped"), Some(TaskState::Skipped));
    assert_eq!(dag.state("independent"), Some(TaskState::Success));
    assert_eq!(error.failures.len(), 1);
    assert_eq!(error.outputs.len(), 1);
    assert_eq!(error.outputs["independent"]["task"], "independent");
    println!(
        "Partial success: states={:?}, outputs={:?}",
        dag.states(),
        error.outputs
    );
    Ok(())
}
