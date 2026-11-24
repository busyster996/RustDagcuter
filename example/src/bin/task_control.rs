use async_trait::async_trait;
use rs_dagcuter::*;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

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

    fn dependencies(&self) -> DependencySpec {
        after(self.deps.iter().map(String::as_str))
    }

    async fn execute(
        &self,
        ctx: TaskContext,
        _attempt: i64,
        _input: &Input,
    ) -> (Output, Option<Error>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match &self.mode {
            Mode::Success => (
                HashMap::from([("task".into(), Arc::new(self.name) as Value)]).into(),
                None,
            ),
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
                (
                    Output::default(),
                    Some(Error::ContextCancelled("task stopped by caller".into())),
                )
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
    let dag = Arc::new(Scheduler::new(
        HashMap::from([("gated".into(), gated), ("free".into(), free)]),
        [],
    )?);
    dag.suspend_task("gated")?;
    dag.suspend();
    let runner = dag.clone();
    let handle = tokio::spawn(async move { runner.execute(Context::new()).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(dag.progress().pending, 2);
    dag.resume();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(gated_calls.load(Ordering::SeqCst), 0);
    assert!(dag.suspended_task("gated"));
    dag.resume_task("gated")?;
    let (outputs, err) = tokio::time::timeout(Duration::from_secs(2), handle).await??;
    assert!(err.is_none());
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
    let dag = Arc::new(Scheduler::new(
        HashMap::from([
            ("target".into(), target),
            ("child".into(), child),
            ("independent".into(), independent),
        ]),
        [],
    )?);
    let runner = dag.clone();
    let handle = tokio::spawn(async move { runner.execute(Context::new()).await });
    started.notified().await;
    let cause: CancelCause = Arc::new(OperatorReason);
    dag.cancel_task("target", &[with_cause(cause)])?;
    let (outputs, error) = tokio::time::timeout(Duration::from_secs(2), handle).await??;
    assert_eq!(error.expect("target is canceled").failures.len(), 1);
    assert_eq!(dag.state("target"), State::Canceled);
    assert_eq!(dag.state("child"), State::Canceled);
    assert_eq!(dag.state("independent"), State::Success);
    assert_eq!(child_calls.load(Ordering::SeqCst), 0);
    assert!(outputs.contains_key("independent"));
    let reason = &reasons.lock().unwrap()[0];
    assert_eq!(reason.kind, CancelKind::Task);
    assert_eq!(
        reason.source.as_ref().unwrap().to_string(),
        "operator canceled this task"
    );
    println!("Task cancellation: states={:?}", dag.states());
    Ok(())
}
