pub mod task;
pub mod retry;
pub mod cycle_check;
pub mod executor;

pub use task::Task;
pub use retry::{RetryPolicy, RetryExecutor};
pub use executor::Dagcuter;
pub use cycle_check::has_cycle;

use std::collections::HashMap;
use thiserror::Error;
use std::sync::Arc;

pub type TaskResult = HashMap<String, serde_json::Value>;
pub type TaskInput = HashMap<String, serde_json::Value>;
pub type BoxTask = Arc<dyn Task>;

#[derive(Error, Debug)]
pub enum DagcuterError {
    #[error("Circular dependency detected")]
    CircularDependency,
    #[error("Task execution failed: {0}")]
    TaskExecution(String),
    #[error("Context cancelled: {0}")]
    ContextCancelled(String),
    #[error("Retry failed: {0}")]
    RetryFailed(String),
}

