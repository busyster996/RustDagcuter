use async_trait::async_trait;
use rs_dagcuter::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{oneshot, Notify};

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

    async fn execute(
        &self,
        ctx: TaskContext,
        _attempt: i64,
        _input: &Input,
    ) -> (Output, Option<Error>) {
        match &self.mode {
            Mode::Cooperative(started) => {
                started.notify_one();
                ctx.cancelled().await;
                assert_eq!(ctx.cause().unwrap().kind, CancelKind::Run);
                (
                    Output::default(),
                    Some(Error::ContextCancelled("work stopped cooperatively".into())),
                )
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
                (
                    HashMap::from([("late_success".into(), Arc::new(true) as Value)]).into(),
                    None,
                )
            }
        }
    }
}

fn dag(mode: Mode) -> Result<Arc<Scheduler>, Error> {
    let task: BoxTask = Arc::new(Step { mode });
    Ok(Arc::new(Scheduler::new(
        HashMap::from([("work".into(), task)]),
        [],
    )?))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let started = Arc::new(Notify::new());
    let cooperative = dag(Mode::Cooperative(started.clone()))?;
    let runner = cooperative.clone();
    let handle = tokio::spawn(async move { runner.execute(Context::new()).await });
    started.notified().await;
    cooperative
        .cancel(Context::new().with_timeout(Duration::from_millis(500)), &[])
        .await?;
    let (outputs, error) = tokio::time::timeout(Duration::from_secs(2), handle).await??;
    assert!(outputs.is_empty());
    let error = error.expect("run was canceled");
    assert_eq!(cooperative.phase(), Phase::Canceled);
    assert_eq!(cooperative.state("work"), State::Canceled);
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
    let handle = tokio::spawn(async move { runner.execute(Context::new()).await });
    started.notified().await;
    assert!(matches!(
        forced
            .cancel(Context::new().with_timeout(Duration::from_millis(25)), &[])
            .await,
        Err(Error::DeadlineExceeded)
    ));
    let (outputs, error) = tokio::time::timeout(Duration::from_secs(2), handle).await??;
    assert!(error.is_some());
    assert_eq!(forced.state("work"), State::Canceled);
    assert!(!outputs.contains_key("work"));
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), finished_rx).await??;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(forced.state("work"), State::Canceled);
    println!(
        "Grace expired: late success discarded, state={:?}",
        forced.state("work")
    );
    Ok(())
}
