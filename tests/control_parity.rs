use async_trait::async_trait;
use rs_dagcuter::*;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Notify;

struct Wait(Arc<Notify>);

#[async_trait]
impl Task for Wait {
    fn name(&self) -> &str {
        "work"
    }

    async fn execute(&self, ctx: TaskContext, _: i64, _: &Input) -> (Output, Option<Error>) {
        self.0.notify_one();
        ctx.cancelled().await;
        (
            Output::default(),
            Some(Error::ContextCancelled("task observed cancellation".into())),
        )
    }
}

fn scheduler(started: Arc<Notify>) -> Scheduler {
    Scheduler::new(
        HashMap::from([("work".into(), Arc::new(Wait(started)) as BoxTask)]),
        [],
    )
    .unwrap()
}

#[tokio::test]
async fn deadline_before_execute_sets_no_attempts_and_reports_the_deadline() {
    let engine = scheduler(Arc::new(Notify::new()));
    assert_eq!(engine.phase(), Phase::Pending);
    assert_eq!(engine.state("not registered"), State::Pending);
    let (outputs, err) = engine
        .execute(Context::new().with_timeout(Duration::ZERO))
        .await;
    assert!(outputs.is_empty());
    assert_eq!(engine.results()["work"].attempts, 0);
    assert_eq!(engine.phase().to_string(), "canceled");
    assert!(matches!(
        err.unwrap().failures[0].error.as_ref(),
        Error::DeadlineExceeded
    ));
    let (_, err) = engine.execute(Context::new()).await;
    assert!(err.unwrap().is_already_executed());
    assert_eq!(engine.phase(), Phase::Canceled);
}

#[tokio::test]
async fn cancel_before_execute_is_immediate_and_applies_the_caller_cause() {
    let engine = scheduler(Arc::new(Notify::new()));
    let control: &dyn Control = &engine;
    let cause = Arc::new(std::io::Error::other("operator stopped the run"));
    control
        .cancel(Context::new(), &[with_cause(cause)])
        .await
        .unwrap();
    assert!(control.canceled());
    let api: &dyn SchedulerApi = &engine;
    let (_, err) = api.execute(Context::new()).await;
    let err = err.unwrap();
    let Error::RunCanceled { reason, .. } = err.failures[0].error.as_ref() else {
        panic!("{err:?}");
    };
    assert_eq!(
        reason.source.as_ref().unwrap().to_string(),
        "operator stopped the run"
    );
    assert_eq!(api.results()["work"].attempts, 0);
}

#[tokio::test]
async fn execute_deadline_interrupts_a_suspended_task() {
    let engine = scheduler(Arc::new(Notify::new()));
    engine.suspend();
    let (_, err) = tokio::time::timeout(
        Duration::from_secs(5),
        engine.execute(Context::new().with_timeout(Duration::from_millis(10))),
    )
    .await
    .unwrap();
    assert!(matches!(
        err.unwrap().failures[0].error.as_ref(),
        Error::DeadlineExceeded
    ));
    assert_eq!(engine.results()["work"].attempts, 0);
}

struct IgnoreCancel {
    started: Arc<Notify>,
    release: Arc<Notify>,
    returned: Arc<Notify>,
}

#[async_trait]
impl Task for IgnoreCancel {
    fn name(&self) -> &str {
        "work"
    }

    async fn execute(&self, ctx: TaskContext, _: i64, _: &Input) -> (Output, Option<Error>) {
        self.started.notify_one();
        self.release.notified().await;
        assert_eq!(ctx.cause().unwrap().source.unwrap().to_string(), "stop");
        self.returned.notify_one();
        (Output::default(), None)
    }
}

#[tokio::test]
async fn expired_grace_reports_the_run_sentinel_without_the_caller_cause() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let returned = Arc::new(Notify::new());
    let engine = Arc::new(
        Scheduler::new(
            HashMap::from([(
                "work".into(),
                Arc::new(IgnoreCancel {
                    started: started.clone(),
                    release: release.clone(),
                    returned: returned.clone(),
                }) as BoxTask,
            )]),
            [],
        )
        .unwrap(),
    );
    let runner = engine.clone();
    let run = tokio::spawn(async move { runner.execute(Context::new()).await });
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(5),
            engine.cancel(
                Context::new().with_timeout(Duration::ZERO),
                &[with_cause(Arc::new(std::io::Error::other("stop")))],
            )
        )
        .await
        .unwrap(),
        Err(Error::DeadlineExceeded)
    ));
    let (outputs, error) = run.await.unwrap();
    assert!(outputs.is_empty());
    assert_eq!(engine.phase(), Phase::Canceled);
    let status = &engine.results()["work"];
    assert_eq!(status.state, State::Canceled);
    assert_eq!(status.attempts, 1);
    let error = error.unwrap();
    assert_eq!(error.failures.len(), 1);
    assert!(matches!(
        error.failures[0].error.as_ref(),
        Error::RunCanceled { reason, last: None }
            if reason.kind == CancelKind::Run && reason.source.is_none()
    ));
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), returned.notified())
        .await
        .unwrap();
    assert_eq!(engine.state("work"), State::Canceled);
    assert_eq!(engine.success_completion_order(), "\n");
}
