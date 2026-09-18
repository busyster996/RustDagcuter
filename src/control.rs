use crate::Error;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub type CancelCause = Arc<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelKind {
    Task,
    Run,
    Parent,
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
        Some(
            self.control
                .reason()
                .unwrap_or_else(|| CancelReason::new(CancelKind::Parent, None)),
        )
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
            _ => underlying
                .unwrap_or_else(|| Error::ContextCancelled("parent context canceled".into())),
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
    pub(crate) attempts: AtomicU64,
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
            attempts: AtomicU64::new(0),
        }
    }

    pub(crate) fn bind(self: &Arc<Self>, parent: &CancellationToken) -> TaskContext {
        let token = parent.child_token();
        let mut state = self.state.lock().unwrap();
        if state.reason.is_some() {
            token.cancel();
        }
        state.token = token.clone();
        TaskContext {
            token,
            control: self.clone(),
        }
    }

    pub(crate) fn cancel(&self, requested: CancelReason) {
        let mut state = self.state.lock().unwrap();
        if state.reason.is_none() {
            state.reason = Some(if state.token.is_cancelled() {
                CancelReason::new(CancelKind::Parent, None)
            } else {
                requested
            });
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
