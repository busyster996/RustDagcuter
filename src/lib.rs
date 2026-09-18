mod control;
mod executor;
mod observer;
mod options;
mod progress;
mod retry;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

pub type TaskResult = HashMap<String, serde_json::Value>;
pub type TaskInput = HashMap<String, serde_json::Value>;
pub type BoxTask = Arc<dyn Task>;
pub const INFINITE_ATTEMPTS: i64 = -1;

#[derive(Error, Debug)]
pub enum Error {
    #[error("circular dependency detected: {0}")]
    CircularDependency(String),
    #[error("task {task:?} depends on unknown task {dependency:?}")]
    UnknownDependency { task: String, dependency: String },
    #[error("task map key {key:?} does not match task name {name:?}")]
    TaskNameMismatch { key: String, name: String },
    #[error("this DAG has already been executed")]
    AlreadyExecuted,
    #[error("task execution failed: {0}")]
    TaskExecution(String),
    #[error("execution canceled: {0}")]
    ContextCancelled(String),
    #[error("retries exhausted: {last}")]
    RetryFailed {
        #[source]
        last: Box<Error>,
    },
    #[error("non-retryable error: {0}")]
    NonRetryable(#[source] Box<Error>),
    #[error("execute: {execute}; post_execution: {post}")]
    TaskAndPostExecution {
        #[source]
        execute: Box<Error>,
        post: Box<Error>,
    },
    #[error("execution canceled during retry wait; last attempt: {last}")]
    RetryInterrupted {
        #[source]
        last: Box<Error>,
    },
    #[error("task {task:?} panicked on attempt {attempt}: {message}")]
    TaskPanic {
        task: String,
        attempt: u64,
        message: String,
        stack: String,
        #[source]
        source: Option<Box<Error>>,
    },
    #[error("observer panicked on event for task {task:?}: {message}")]
    ObserverPanic {
        task: String,
        message: String,
        stack: String,
    },
    #[error("unknown task {0:?}")]
    UnknownTask(String),
    #[error("task {0:?} already reached a terminal state")]
    TaskAlreadyDone(String),
    #[error("task {task:?} canceled")]
    TaskCanceled {
        task: String,
        reason: CancelReason,
        #[source]
        last: Option<Box<Error>>,
    },
    #[error("run canceled")]
    RunCanceled {
        reason: CancelReason,
        #[source]
        last: Option<Box<Error>>,
    },
    #[error("cancel grace period expired")]
    GracePeriodExpired,
}

impl Error {
    pub(crate) fn is_non_retryable(&self) -> bool {
        match self {
            Self::NonRetryable(_) => true,
            Self::TaskAndPostExecution { execute, post } => {
                execute.is_non_retryable() || post.is_non_retryable()
            }
            Self::RetryFailed { last } | Self::RetryInterrupted { last } => last.is_non_retryable(),
            Self::TaskPanic { source, .. } => source.as_deref().is_some_and(Self::is_non_retryable),
            _ => false,
        }
    }

    pub(crate) fn is_run_canceled(&self) -> bool {
        match self {
            Self::RunCanceled { .. } => true,
            Self::RetryFailed { last }
            | Self::RetryInterrupted { last }
            | Self::NonRetryable(last) => last.is_run_canceled(),
            Self::TaskAndPostExecution { execute, post } => {
                execute.is_run_canceled() || post.is_run_canceled()
            }
            Self::TaskPanic { source, .. } => source.as_deref().is_some_and(Self::is_run_canceled),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Success,
    Skipped,
    Canceled,
    Failed,
}

impl TaskState {
    pub fn done(self) -> bool {
        self != Self::Pending
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunPhase {
    Pending,
    Running,
    Success,
    Canceled,
    Failed,
}

impl RunPhase {
    pub fn done(self) -> bool {
        matches!(self, Self::Success | Self::Canceled | Self::Failed)
    }
}

#[derive(Debug, Clone)]
pub struct TaskStatus {
    pub state: TaskState,
    pub error: Option<Arc<Error>>,
    pub attempts: u64,
}

#[derive(Debug, Clone)]
pub struct TaskFailure {
    pub task: String,
    pub error: Arc<Error>,
}

#[derive(Debug)]
pub struct RunError {
    pub outputs: HashMap<String, TaskResult>,
    pub failures: Vec<TaskFailure>,
    pub observer_failures: Vec<ObserverFailure>,
    pub run_error: Option<Box<Error>>,
}

impl RunError {
    pub fn is_already_executed(&self) -> bool {
        matches!(self.run_error.as_deref(), Some(Error::AlreadyExecuted))
    }
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(error) = &self.run_error {
            return error.fmt(f);
        }
        if self.failures.is_empty() {
            write!(
                f,
                "{} observer callback(s) panicked",
                self.observer_failures.len()
            )
        } else if self.observer_failures.is_empty() {
            write!(f, "{} task(s) failed or were canceled", self.failures.len())
        } else {
            write!(
                f,
                "{} task(s) failed or were canceled; {} observer callback(s) panicked",
                self.failures.len(),
                self.observer_failures.len()
            )
        }
    }
}

impl std::error::Error for RunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.run_error
            .as_deref()
            .map(|error| error as &dyn std::error::Error)
            .or_else(|| {
                self.failures
                    .first()
                    .map(|failure| failure.error.as_ref() as _)
            })
            .or_else(|| {
                self.observer_failures
                    .first()
                    .map(|failure| failure.error.as_ref() as _)
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub interval: Duration,
    pub max_interval: Duration,
    /// Total attempts; zero means one attempt, and a negative value means unlimited attempts.
    pub max_attempts: i64,
    pub multiplier: f64,
    pub jitter: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(30),
            max_attempts: 1,
            multiplier: 2.0,
            jitter: 0.0,
        }
    }
}

#[derive(Debug)]
pub struct TaskOutcome {
    pub output: Option<TaskResult>,
    pub error: Option<Error>,
}

impl TaskOutcome {
    pub fn success(output: TaskResult) -> Self {
        Self {
            output: Some(output),
            error: None,
        }
    }

    pub fn failure(error: Error) -> Self {
        Self {
            output: None,
            error: Some(error),
        }
    }

    pub fn with_error(output: TaskResult, error: Error) -> Self {
        Self {
            output: Some(output),
            error: Some(error),
        }
    }
}

#[async_trait]
pub trait Task: Send + Sync {
    fn name(&self) -> &str;
    fn dependencies(&self) -> Vec<String>;
    fn retry_policy(&self) -> Option<RetryPolicy>;

    async fn pre_execution(
        &self,
        _ctx: TaskContext,
        _attempt: u64,
        _input: &TaskInput,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn execute(&self, ctx: TaskContext, attempt: u64, input: &TaskInput) -> TaskOutcome;

    async fn post_execution(
        &self,
        _ctx: TaskContext,
        _attempt: u64,
        _output: Option<&TaskResult>,
        _error: Option<&Error>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

pub use control::{CancelCause, CancelKind, CancelReason, TaskContext};
pub use executor::{validate, Dag};
pub use observer::{Event, ObserverFailure};
pub use options::DagOptions;
pub use progress::Progress;
