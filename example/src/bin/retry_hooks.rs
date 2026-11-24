use async_trait::async_trait;
use rs_dagcuter::*;
use std::collections::HashMap;
use std::panic::panic_any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

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

    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy {
            max_attempts: match self.mode {
                Mode::Transient => 2,
                Mode::Permanent => 3,
                Mode::OutputError => 1,
                Mode::Panic => 5,
            },
            interval: Duration::from_millis(20),
            jitter: 0.2,
            ..RetryPolicy::default()
        }
    }

    async fn pre_execution(&self, _ctx: TaskContext, attempt: i64, _input: &Input) {
        self.hooks.pre.fetch_add(1, Ordering::SeqCst);
        println!("{}: pre_execution attempt {attempt}", self.name);
    }

    async fn execute(
        &self,
        _ctx: TaskContext,
        attempt: i64,
        _input: &Input,
    ) -> (Output, Option<Error>) {
        match self.mode {
            Mode::Transient if attempt == 1 => (
                Output::default(),
                Some(Error::TaskExecution("temporary failure".into())),
            ),
            Mode::Transient => (
                HashMap::from([("attempt".into(), Arc::new(attempt) as Value)]).into(),
                None,
            ),
            Mode::Permanent => (
                Output::default(),
                Some(Error::NonRetryable(Box::new(Error::TaskExecution(
                    "invalid request".into(),
                )))),
            ),
            Mode::OutputError => (
                Data::from([("partial".into(), Arc::new(true) as Value)]).into(),
                Some(Error::TaskExecution("output and error".into())),
            ),
            Mode::Panic => panic_any(Error::NonRetryable(Box::new(Error::TaskExecution(
                "expected task panic with non-retryable cause".into(),
            )))),
        }
    }

    async fn post_execution(
        &self,
        _ctx: TaskContext,
        attempt: i64,
        output: &Output,
        error: Option<&Error>,
    ) {
        self.hooks.post.fetch_add(1, Ordering::SeqCst);
        if !output.data.is_empty() && error.is_some() {
            self.hooks.both.fetch_add(1, Ordering::SeqCst);
        }
        println!(
            "{}: post_execution attempt {attempt}, output={}, error={}",
            self.name,
            !output.data.is_empty(),
            error.is_some()
        );
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
    let dag = Scheduler::new(
        HashMap::from([
            ("transient".into(), transient),
            ("permanent".into(), permanent),
            ("partial".into(), partial),
            ("panic".into(), panic_step),
        ]),
        [],
    )?;
    println!(
        "One task panic is intentional; Rust's panic hook prints it before the DAG catches it."
    );
    let (outputs, error) = dag.execute(Context::new()).await;
    let error = error.expect("three tasks deliberately fail");
    let statuses = dag.results();
    assert_eq!(statuses["transient"].state, State::Success);
    assert_eq!(statuses["transient"].attempts, 2);
    assert_eq!(
        outputs["transient"]["attempt"]
            .downcast_ref::<i64>()
            .copied(),
        Some(2)
    );
    assert_eq!(statuses["permanent"].attempts, 1);
    assert!(matches!(
        statuses["permanent"].err.as_deref(),
        Some(Error::NonRetryable(_))
    ));
    assert_eq!(statuses["panic"].attempts, 1);
    assert!(matches!(
        statuses["panic"].err.as_deref(),
        Some(Error::TaskPanic { source: Some(source), .. })
            if matches!(source.as_ref(), Error::NonRetryable(_))
    ));
    assert_eq!(statuses["partial"].state, State::Failed);
    assert!(!outputs.contains_key("partial"));
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
        outputs.len(),
        error.failures.len()
    );
    Ok(())
}
