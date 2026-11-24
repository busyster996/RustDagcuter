use crate::Error;
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub type CancelCause = Arc<dyn std::error::Error + Send + Sync>;

/// A Tokio cancellation token with an optional deadline.
#[derive(Clone, Debug, Default)]
pub struct Context {
    token: CancellationToken,
    pub(crate) deadline: Option<tokio::time::Instant>,
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_deadline(self, deadline: tokio::time::Instant) -> Self {
        let deadline = self.deadline.map_or(deadline, |prior| prior.min(deadline));
        Self {
            deadline: Some(deadline),
            ..self
        }
    }

    pub fn with_timeout(self, timeout: std::time::Duration) -> Self {
        let Some(deadline) = tokio::time::Instant::now().checked_add(timeout) else {
            return self;
        };
        self.with_deadline(deadline)
    }

    pub fn cancel(&self) {
        self.token.cancel();
    }

    pub(crate) fn token(&self) -> &CancellationToken {
        &self.token
    }

    pub(crate) fn error(&self) -> Option<Error> {
        if self.token.is_cancelled() {
            Some(Error::ContextCancelled("context canceled".into()))
        } else if self
            .deadline
            .is_some_and(|at| tokio::time::Instant::now() >= at)
        {
            Some(Error::DeadlineExceeded)
        } else {
            None
        }
    }

    pub async fn done(&self) -> Error {
        if let Some(err) = self.error() {
            return err;
        }
        if let Some(at) = self.deadline {
            tokio::select! {
                biased;
                _ = self.token.cancelled() => Error::ContextCancelled("context canceled".into()),
                _ = tokio::time::sleep_until(at) => Error::DeadlineExceeded,
            }
        } else {
            self.token.cancelled().await;
            Error::ContextCancelled("context canceled".into())
        }
    }
}

impl From<CancellationToken> for Context {
    fn from(token: CancellationToken) -> Self {
        Self {
            token,
            deadline: None,
        }
    }
}

#[derive(Clone)]
pub struct CancelOption(CancelCause);

pub fn with_cause(cause: CancelCause) -> CancelOption {
    CancelOption(cause)
}

pub(crate) fn cause_of(options: &[CancelOption]) -> Option<CancelCause> {
    options.last().map(|option| option.0.clone())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelKind {
    Task,
    Run,
    Parent,
    TaskDone,
}

#[derive(Debug, Clone)]
pub struct CancelReason {
    pub kind: CancelKind,
    pub source: Option<CancelCause>,
}

impl CancelReason {
    pub(crate) fn new(kind: CancelKind, source: Option<CancelCause>) -> Self {
        Self { kind, source }
    }
}

#[derive(Clone)]
pub struct TaskContext {
    token: CancellationToken,
    control: Arc<TaskControl>,
    parent: Context,
}

impl TaskContext {
    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub fn cause(&self) -> Option<CancelReason> {
        if !self.is_cancelled() {
            return None;
        }
        Some(self.control.reason().unwrap_or_else(|| {
            CancelReason::new(
                CancelKind::Parent,
                self.parent.error().map(|err| Arc::new(err) as CancelCause),
            )
        }))
    }

    pub(crate) fn release_on_drop(&self) -> impl Drop {
        struct Release(Arc<TaskControl>);
        impl Drop for Release {
            fn drop(&mut self) {
                let mut state = self.0.state.lock().unwrap();
                if !state.token.is_cancelled() {
                    state.reason = Some(CancelReason::new(CancelKind::TaskDone, None));
                    state.token.cancel();
                }
            }
        }
        Release(self.control.clone())
    }

    pub(crate) fn token(&self) -> &CancellationToken {
        &self.token
    }

    pub(crate) fn control(&self) -> &Arc<TaskControl> {
        &self.control
    }

    pub(crate) fn cancellation_error(&self, underlying: Option<Error>) -> Error {
        match self.cause().map(|reason| reason.kind) {
            Some(CancelKind::Task) => Error::TaskCanceled {
                task: self.control.name.clone(),
                reason: self.cause().unwrap(),
                last: underlying.map(Box::new),
            },
            Some(CancelKind::Run) => Error::RunCanceled {
                reason: self.cause().unwrap(),
                last: underlying.map(Box::new),
            },
            _ => match (self.parent.error(), underlying) {
                (Some(Error::DeadlineExceeded), Some(Error::RetryInterrupted { last })) => {
                    Error::DeadlineInterrupted { last }
                }
                (Some(Error::DeadlineExceeded), _) => Error::DeadlineExceeded,
                (_, underlying) => underlying
                    .unwrap_or_else(|| Error::ContextCancelled("parent context canceled".into())),
            },
        }
    }
}

struct ControlState {
    token: CancellationToken,
    reason: Option<CancelReason>,
    task_suspended: bool,
    run_suspended: bool,
}

pub(crate) struct TaskControl {
    name: String,
    state: Mutex<ControlState>,
    paused: watch::Sender<bool>,
    pub(crate) attempts: AtomicI64,
}

impl TaskControl {
    pub(crate) fn new(name: String) -> Self {
        let (paused, _) = watch::channel(false);
        Self {
            name,
            state: Mutex::new(ControlState {
                token: CancellationToken::new(),
                reason: None,
                task_suspended: false,
                run_suspended: false,
            }),
            paused,
            attempts: AtomicI64::new(0),
        }
    }

    pub(crate) fn bind(
        self: &Arc<Self>,
        token: &CancellationToken,
        parent: &Context,
    ) -> TaskContext {
        let token = token.child_token();
        let mut state = self.state.lock().unwrap();
        if state.reason.is_some() {
            token.cancel();
        }
        state.token = token.clone();
        TaskContext {
            token,
            control: self.clone(),
            parent: parent.clone(),
        }
    }

    pub(crate) fn cancel(&self, requested: CancelReason) {
        let mut state = self.state.lock().unwrap();
        if state.reason.is_none() && !state.token.is_cancelled() {
            state.reason = Some(requested);
        }
        state.token.cancel();
    }

    pub(crate) fn reason(&self) -> Option<CancelReason> {
        self.state.lock().unwrap().reason.clone()
    }

    pub(crate) fn set_task_suspended(&self, suspended: bool) {
        let mut state = self.state.lock().unwrap();
        if state.task_suspended != suspended {
            state.task_suspended = suspended;
            self.paused
                .send_replace(state.task_suspended || state.run_suspended);
        }
    }

    pub(crate) fn set_run_suspended(&self, suspended: bool) {
        let mut state = self.state.lock().unwrap();
        if state.run_suspended != suspended {
            state.run_suspended = suspended;
            self.paused
                .send_replace(state.task_suspended || state.run_suspended);
        }
    }

    pub(crate) fn suspended(&self) -> bool {
        *self.paused.borrow()
    }

    pub(crate) async fn wait(&self, token: &CancellationToken) -> Result<(), Error> {
        let mut paused = self.paused.subscribe();
        loop {
            if token.is_cancelled() {
                return Err(Error::ContextCancelled("canceled while suspended".into()));
            }
            if !*paused.borrow_and_update() {
                return Ok(());
            }
            tokio::select! {
                _ = token.cancelled() => return Err(Error::ContextCancelled("canceled while suspended".into())),
                result = paused.changed() => {
                    if result.is_err() {
                        return Err(Error::TaskExecution("suspension gate closed".into()));
                    }
                }
            }
        }
    }
}
