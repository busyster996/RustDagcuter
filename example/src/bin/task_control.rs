use async_trait::async_trait;
use rs_dagcuter::{
    BoxTask, CancelCause, CancelKind, CancelReason, Dag, Error, RetryPolicy, Task, TaskContext,
    TaskInput, TaskOutcome, TaskState,
};
use serde_json::json;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

enum Mode {
    Success,
    WaitForCancel(Arc<Notify>, Arc<Mutex<Vec<CancelReason>>>),
}

struct Step {
    name: &'static str,
    deps: Vec<String>,
    mode: Mode,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Task for Step {
    fn name(&self) -> &str {
        self.name
    }

    fn dependencies(&self) -> Vec<String> {
        self.deps.clone()
    }

    fn retry_policy(&self) -> Option<RetryPolicy> {
        None
    }

    async fn execute(&self, ctx: TaskContext, _attempt: u64, _input: &TaskInput) -> TaskOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match &self.mode {
            Mode::Success => {
                TaskOutcome::success(HashMap::from([("task".into(), json!(self.name))]))
            }
            Mode::WaitForCancel(started, reasons) => {
                started.notify_one();
                ctx.cancelled().await;
                let reason = ctx.cause().expect("canceled context has a cause");
                println!(
                    "{}: canceled by {:?}, custom={}",
                    self.name,
                    reason.kind,
                    reason.source.is_some()
                );
                reasons.lock().unwrap().push(reason);
                TaskOutcome::failure(Error::ContextCancelled("task stopped by caller".into()))
            }
        }
    }
}

fn step(name: &'static str, deps: &[&str], mode: Mode) -> (BoxTask, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    (
        Arc::new(Step {
            name,
            deps: deps.iter().map(|dependency| (*dependency).into()).collect(),
            mode,
            calls: calls.clone(),
        }),
        calls,
    )
}

#[derive(Debug)]
struct OperatorReason;

impl fmt::Display for OperatorReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("operator canceled this task")
    }
}

impl StdError for OperatorReason {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let (gated, gated_calls) = step("gated", &[], Mode::Success);
    let (free, _) = step("free", &[], Mode::Success);
    let dag = Arc::new(Dag::new(HashMap::from([
        ("gated".into(), gated),
        ("free".into(), free),
    ]))?);
    dag.suspend_task("gated")?;
    dag.suspend();
    let runner = dag.clone();
    let handle = tokio::spawn(async move { runner.execute(CancellationToken::new()).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(dag.progress().pending, 2);
    dag.resume();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(gated_calls.load(Ordering::SeqCst), 0);
    assert!(dag.suspended_task("gated"));
    dag.resume_task("gated")?;
    let outputs = tokio::time::timeout(Duration::from_secs(2), handle)
        .await??
        .expect("both suspended tasks should succeed");
    assert_eq!(outputs.len(), 2);
    println!(
        "Suspension: both sources resumed, {} tasks succeeded",
        outputs.len()
    );

    let started = Arc::new(Notify::new());
    let reasons = Arc::new(Mutex::new(Vec::new()));
    let (target, _) = step(
        "target",
        &[],
        Mode::WaitForCancel(started.clone(), reasons.clone()),
    );
    let (child, child_calls) = step("child", &["target"], Mode::Success);
    let (independent, _) = step("independent", &[], Mode::Success);
    let dag = Arc::new(Dag::new(HashMap::from([
        ("target".into(), target),
        ("child".into(), child),
        ("independent".into(), independent),
    ]))?);
    let runner = dag.clone();
    let handle = tokio::spawn(async move { runner.execute(CancellationToken::new()).await });
    started.notified().await;
    let cause: CancelCause = Arc::new(OperatorReason);
    dag.cancel_task_with_cause("target", cause)?;
    let error = tokio::time::timeout(Duration::from_secs(2), handle)
        .await??
        .expect_err("target is canceled");
    assert_eq!(dag.state("target"), Some(TaskState::Canceled));
    assert_eq!(dag.state("child"), Some(TaskState::Canceled));
    assert_eq!(dag.state("independent"), Some(TaskState::Success));
    assert_eq!(child_calls.load(Ordering::SeqCst), 0);
    assert!(error.outputs.contains_key("independent"));
    let reason = &reasons.lock().unwrap()[0];
    assert_eq!(reason.kind, CancelKind::Task);
    assert_eq!(
        reason.source.as_ref().unwrap().to_string(),
        "operator canceled this task"
    );
    println!("Task cancellation: states={:?}", dag.states());
    Ok(())
}
