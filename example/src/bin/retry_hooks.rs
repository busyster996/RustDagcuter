use async_trait::async_trait;
use rs_dagcuter::{
    BoxTask, Dag, Error, RetryPolicy, Task, TaskContext, TaskInput, TaskOutcome, TaskResult,
    TaskState,
};
use serde_json::json;
use std::collections::HashMap;
use std::panic::panic_any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
enum Mode {
    Transient,
    Permanent,
    OutputError,
    Panic,
}

#[derive(Default)]
struct HookCounts {
    pre: AtomicUsize,
    post: AtomicUsize,
    both: AtomicUsize,
}

struct Step {
    name: &'static str,
    mode: Mode,
    hooks: Arc<HookCounts>,
}

#[async_trait]
impl Task for Step {
    fn name(&self) -> &str {
        self.name
    }

    fn dependencies(&self) -> Vec<String> {
        Vec::new()
    }

    fn retry_policy(&self) -> Option<RetryPolicy> {
        Some(RetryPolicy {
            max_attempts: match self.mode {
                Mode::Transient => 2,
                Mode::Permanent => 3,
                Mode::OutputError => 1,
                Mode::Panic => 5,
            },
            interval: Duration::from_millis(20),
            jitter: 0.2,
            ..RetryPolicy::default()
        })
    }

    async fn pre_execution(
        &self,
        _ctx: TaskContext,
        attempt: u64,
        _input: &TaskInput,
    ) -> Result<(), Error> {
        self.hooks.pre.fetch_add(1, Ordering::SeqCst);
        println!("{}: pre_execution attempt {attempt}", self.name);
        Ok(())
    }

    async fn execute(&self, _ctx: TaskContext, attempt: u64, _input: &TaskInput) -> TaskOutcome {
        match self.mode {
            Mode::Transient if attempt == 1 => {
                TaskOutcome::failure(Error::TaskExecution("temporary failure".into()))
            }
            Mode::Transient => {
                TaskOutcome::success(HashMap::from([("attempt".into(), json!(attempt))]))
            }
            Mode::Permanent => TaskOutcome::failure(Error::NonRetryable(Box::new(
                Error::TaskExecution("invalid request".into()),
            ))),
            Mode::OutputError => TaskOutcome::with_error(
                HashMap::from([("partial".into(), json!(true))]),
                Error::TaskExecution("output and error".into()),
            ),
            Mode::Panic => panic_any(Error::NonRetryable(Box::new(Error::TaskExecution(
                "expected task panic with non-retryable cause".into(),
            )))),
        }
    }

    async fn post_execution(
        &self,
        _ctx: TaskContext,
        attempt: u64,
        output: Option<&TaskResult>,
        error: Option<&Error>,
    ) -> Result<(), Error> {
        self.hooks.post.fetch_add(1, Ordering::SeqCst);
        if output.is_some() && error.is_some() {
            self.hooks.both.fetch_add(1, Ordering::SeqCst);
        }
        println!(
            "{}: post_execution attempt {attempt}, output={}, error={}",
            self.name,
            output.is_some(),
            error.is_some()
        );
        Ok(())
    }
}

fn step(name: &'static str, mode: Mode) -> (BoxTask, Arc<HookCounts>) {
    let hooks = Arc::new(HookCounts::default());
    (
        Arc::new(Step {
            name,
            mode,
            hooks: hooks.clone(),
        }),
        hooks,
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (transient, transient_hooks) = step("transient", Mode::Transient);
    let (permanent, permanent_hooks) = step("permanent", Mode::Permanent);
    let (partial, partial_hooks) = step("partial", Mode::OutputError);
    let (panic_step, panic_hooks) = step("panic", Mode::Panic);
    let dag = Dag::new(HashMap::from([
        ("transient".into(), transient),
        ("permanent".into(), permanent),
        ("partial".into(), partial),
        ("panic".into(), panic_step),
    ]))?;
    println!(
        "One task panic is intentional; Rust's panic hook prints it before the DAG catches it."
    );
    let error = dag
        .execute(CancellationToken::new())
        .await
        .expect_err("three tasks deliberately fail");
    let statuses = dag.task_results();
    assert_eq!(statuses["transient"].state, TaskState::Success);
    assert_eq!(statuses["transient"].attempts, 2);
    assert_eq!(error.outputs["transient"]["attempt"], 2);
    assert_eq!(statuses["permanent"].attempts, 1);
    assert!(matches!(
        statuses["permanent"].error.as_deref(),
        Some(Error::NonRetryable(_))
    ));
    assert_eq!(statuses["panic"].attempts, 1);
    assert!(matches!(
        statuses["panic"].error.as_deref(),
        Some(Error::TaskPanic { source: Some(source), .. })
            if matches!(source.as_ref(), Error::NonRetryable(_))
    ));
    assert_eq!(statuses["partial"].state, TaskState::Failed);
    assert!(!error.outputs.contains_key("partial"));
    assert_eq!(partial_hooks.both.load(Ordering::SeqCst), 1);
    assert_eq!(partial_hooks.post.load(Ordering::SeqCst), 1);
    assert_eq!(error.failures.len(), 3);
    assert_eq!(
        (
            transient_hooks.pre.load(Ordering::SeqCst),
            transient_hooks.post.load(Ordering::SeqCst)
        ),
        (2, 2)
    );
    assert_eq!(
        (
            permanent_hooks.pre.load(Ordering::SeqCst),
            permanent_hooks.post.load(Ordering::SeqCst)
        ),
        (1, 1)
    );
    assert_eq!(
        (
            panic_hooks.pre.load(Ordering::SeqCst),
            panic_hooks.post.load(Ordering::SeqCst)
        ),
        (1, 0)
    );
    println!(
        "Retry result: {} success, {} deliberate failures",
        error.outputs.len(),
        error.failures.len()
    );
    Ok(())
}
