mod control;
mod edge;
mod executor;
mod interface;
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

/// Opaque task data, shared read-only through `Arc`. Recover a value with `downcast_ref`.
pub type Data = HashMap<String, Value>;
pub type Value = Arc<dyn std::any::Any + Send + Sync>;
pub type Outputs = HashMap<String, Data>;
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
    #[error("task {0:?} exited without a result")]
    TaskAbandoned(String),
    #[error("task execution failed: {0}")]
    TaskExecution(String),
    #[error(transparent)]
    TaskError(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("task {task:?} emitted the reserved trigger {trigger:?}")]
    InvalidEmit { task: String, trigger: Trigger },
    #[error("execution canceled: {0}")]
    ContextCancelled(String),
    #[error("retries exhausted: {last}")]
    RetryFailed {
        #[source]
        last: Box<Error>,
    },
    #[error("non-retryable error: {0}")]
    NonRetryable(#[source] Box<Error>),
    #[error("execution canceled during retry wait; last attempt: {last}")]
    RetryInterrupted {
        #[source]
        last: Box<Error>,
    },
    #[error("task {task:?} panicked on attempt {attempt}: {message}")]
    TaskPanic {
        task: String,
        attempt: i64,
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
    #[error("context deadline exceeded")]
    DeadlineExceeded,
    #[error("context deadline exceeded: {last}")]
    DeadlineInterrupted {
        #[source]
        last: Box<Error>,
    },
}

impl Error {
    pub(crate) fn is_non_retryable(&self) -> bool {
        match self {
            Self::NonRetryable(_) | Self::InvalidEmit { .. } => true,
            Self::RetryFailed { last } | Self::RetryInterrupted { last } => last.is_non_retryable(),
            Self::TaskPanic { source, .. } => source.as_deref().is_some_and(Self::is_non_retryable),
            _ => false,
        }
    }

    pub(crate) fn is_context_error(&self) -> bool {
        match self {
            Self::ContextCancelled(_)
            | Self::DeadlineExceeded
            | Self::DeadlineInterrupted { .. }
            | Self::RetryInterrupted { .. } => true,
            Self::RetryFailed { last } | Self::NonRetryable(last) => last.is_context_error(),
            Self::TaskPanic { source, .. } => source.as_deref().is_some_and(Self::is_context_error),
            _ => false,
        }
    }

    pub(crate) fn is_interruption(&self) -> bool {
        self.is_context_error()
            || self.is_run_canceled()
            || match self {
                Self::TaskCanceled { .. } => true,
                Self::RetryFailed { last } | Self::NonRetryable(last) => last.is_interruption(),
                Self::TaskPanic { source, .. } => {
                    source.as_deref().is_some_and(Self::is_interruption)
                }
                _ => false,
            }
    }

    pub(crate) fn is_run_canceled(&self) -> bool {
        match self {
            Self::RunCanceled { .. } => true,
            Self::RetryFailed { last }
            | Self::RetryInterrupted { last }
            | Self::NonRetryable(last) => last.is_run_canceled(),
            Self::TaskPanic { source, .. } => source.as_deref().is_some_and(Self::is_run_canceled),
            _ => false,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum State {
    #[default]
    Pending,
    Success,
    Skipped,
    Canceled,
    Failed,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Pending => "pending",
            Self::Success => "success",
            Self::Skipped => "skipped",
            Self::Canceled => "canceled",
            Self::Failed => "failed",
        })
    }
}

impl State {
    pub fn done(self) -> bool {
        self != Self::Pending
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    #[default]
    Pending,
    Running,
    Success,
    Canceled,
    Failed,
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Success => "success",
            Self::Canceled => "canceled",
            Self::Failed => "failed",
        })
    }
}

impl Phase {
    pub fn done(self) -> bool {
        matches!(self, Self::Success | Self::Canceled | Self::Failed)
    }
}

#[derive(Debug, Clone)]
pub struct TaskResult {
    pub state: State,
    pub err: Option<Arc<Error>>,
    pub attempts: i64,
}

#[derive(Debug, Clone)]
pub struct TaskFailure {
    pub task: String,
    pub error: Arc<Error>,
}

#[derive(Debug)]
pub struct RunError {
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
        for (index, err) in self
            .failures
            .iter()
            .map(|entry| &entry.error)
            .chain(self.observer_failures.iter().map(|entry| &entry.error))
            .enumerate()
        {
            if index > 0 {
                writeln!(f)?;
            }
            write!(f, "{err}")?;
        }
        Ok(())
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub interval: Duration,
    pub max_interval: Duration,
    /// Total attempts; zero means one attempt, and a negative value means unlimited attempts.
    pub max_attempts: i64,
    pub multiplier: f64,
    pub jitter: f64,
}

#[async_trait]
pub trait Task: Send + Sync {
    fn name(&self) -> &str;

    fn dependencies(&self) -> DependencySpec {
        DependencySpec::default()
    }

    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::default()
    }

    async fn pre_execution(&self, _ctx: TaskContext, _attempt: i64, _input: &Input) {}

    async fn execute(
        &self,
        ctx: TaskContext,
        attempt: i64,
        input: &Input,
    ) -> (Output, Option<Error>);

    async fn post_execution(
        &self,
        _ctx: TaskContext,
        _attempt: i64,
        _output: &Output,
        _err: Option<&Error>,
    ) {
    }
}

pub use control::{
    with_cause, CancelCause, CancelKind, CancelOption, CancelReason, Context, TaskContext,
};
pub use edge::{
    after, DependencyEdge, DependencySpec, Input, JoinPolicy, Output, Trigger, ON_FAILURE,
    ON_SUCCESS,
};
pub use executor::{validate, Scheduler};
pub use interface::{Control, SchedulerApi};
pub use observer::{Event, ObserverFailure};
pub use options::{with_max_concurrency, with_observer, SchedulerOption};
pub use progress::Progress;
