use async_trait::async_trait;
use rs_dagcuter::{
    BoxTask, CancelKind, Dag, Error, RetryPolicy, RunPhase, Task, TaskContext, TaskInput,
    TaskOutcome, TaskState,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{oneshot, Notify};
use tokio_util::sync::CancellationToken;

enum Mode {
    Cooperative(Arc<Notify>),
    IgnoreUntilReleased {
        started: Arc<Notify>,
        release: Arc<Notify>,
        finished: Mutex<Option<oneshot::Sender<()>>>,
    },
}

struct Step {
    mode: Mode,
}

#[async_trait]
impl Task for Step {
    fn name(&self) -> &str {
        "work"
    }

    fn dependencies(&self) -> Vec<String> {
        Vec::new()
    }

    fn retry_policy(&self) -> Option<RetryPolicy> {
        None
    }

    async fn execute(&self, ctx: TaskContext, _attempt: u64, _input: &TaskInput) -> TaskOutcome {
        match &self.mode {
            Mode::Cooperative(started) => {
                started.notify_one();
                ctx.cancelled().await;
                assert_eq!(ctx.cause().unwrap().kind, CancelKind::Run);
                TaskOutcome::failure(Error::ContextCancelled("work stopped cooperatively".into()))
            }
            Mode::IgnoreUntilReleased {
                started,
                release,
                finished,
            } => {
                started.notify_one();
                release.notified().await;
                if let Some(sender) = finished.lock().unwrap().take() {
                    let _ = sender.send(());
                }
                TaskOutcome::success(HashMap::from([("late_success".into(), json!(true))]))
            }
        }
    }
}

fn dag(mode: Mode) -> Result<Arc<Dag>, Error> {
    let task: BoxTask = Arc::new(Step { mode });
    Ok(Arc::new(Dag::new(HashMap::from([("work".into(), task)]))?))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let started = Arc::new(Notify::new());
    let cooperative = dag(Mode::Cooperative(started.clone()))?;
    let runner = cooperative.clone();
    let handle = tokio::spawn(async move { runner.execute(CancellationToken::new()).await });
    started.notified().await;
    cooperative.cancel(Duration::from_millis(500)).await?;
    let error = tokio::time::timeout(Duration::from_secs(2), handle)
        .await??
        .expect_err("run was canceled");
    assert_eq!(cooperative.phase(), RunPhase::Canceled);
    assert_eq!(cooperative.state("work"), Some(TaskState::Canceled));
    assert_eq!(error.failures.len(), 1);
    println!("Cooperative cancel: {}", error);

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (finished_tx, finished_rx) = oneshot::channel();
    let forced = dag(Mode::IgnoreUntilReleased {
        started: started.clone(),
        release: release.clone(),
        finished: Mutex::new(Some(finished_tx)),
    })?;
    let runner = forced.clone();
    let handle = tokio::spawn(async move { runner.execute(CancellationToken::new()).await });
    started.notified().await;
    assert!(matches!(
        forced.cancel(Duration::from_millis(25)).await,
        Err(Error::GracePeriodExpired)
    ));
    let error = tokio::time::timeout(Duration::from_secs(2), handle)
        .await??
        .expect_err("grace expired");
    assert_eq!(forced.state("work"), Some(TaskState::Canceled));
    assert!(!error.outputs.contains_key("work"));
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), finished_rx).await??;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(forced.state("work"), Some(TaskState::Canceled));
    println!(
        "Grace expired: late success discarded, state={:?}",
        forced.state("work")
    );
    Ok(())
}
