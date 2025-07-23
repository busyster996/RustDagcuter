mod executor;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

/// Type alias for task execution results
/// Uses HashMap to allow flexible key-value output data
pub type TaskResult = HashMap<String, serde_json::Value>;

/// Type alias for task input parameters
/// Provides flexibility for passing various data types between tasks
pub type TaskInput = HashMap<String, serde_json::Value>;

/// Type alias for thread-safe task references
/// Arc enables sharing tasks across threads safely
pub type BoxTask = Arc<dyn Task>;

/// Comprehensive error types for task execution system
/// Uses thiserror for automatic error trait implementations
#[derive(Error, Debug)]
pub enum Error {
    #[error("Circular dependency detected")]
    CircularDependency,
    #[error("Task execution failed: {0}")]
    TaskExecution(String),
    #[error("Context cancelled: {0}")]
    ContextCancelled(String),
    #[error("Retry failed: {0}")]
    RetryFailed(String),
}

/// Configuration for exponential backoff retry mechanism
/// Implements intelligent retry logic with configurable parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Initial delay between retry attempts
    pub interval: Duration,
    /// Maximum delay to prevent excessively long waits
    pub max_interval: Duration,
    /// Maximum number of retry attempts (-1 for unlimited)
    pub max_attempts: i32,
    /// Exponential backoff multiplier (2.0 = double delay each retry)
    pub multiplier: f64,
}

impl Default for RetryPolicy {
    /// Provides sensible defaults for retry behavior
    /// Starts with 1s delay, max 30s, exponential backoff with 2x multiplier
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(30),
            max_attempts: -1, // Unlimited retries by default
            multiplier: 2.0,  // Exponential backoff
        }
    }
}

/// Retry mechanism implementation with exponential backoff
/// Handles transient failures gracefully with configurable policies
struct Retry {
    policy: RetryPolicy,
}

impl Retry {
    /// Creates a new retry instance with validated policy parameters
    /// Applies sensible defaults and boundary checks to prevent invalid configurations
    fn new(policy: Option<RetryPolicy>) -> Self {
        let mut policy = policy.unwrap_or_default();

        // Validate and set safe default values to prevent edge cases
        if policy.interval.is_zero() {
            policy.interval = Duration::from_secs(1);
        }
        if policy.max_interval.is_zero() {
            policy.max_interval = Duration::from_secs(30);
        }
        if policy.multiplier <= 0.0 {
            policy.multiplier = 2.0;
        }
        // Prevent extremely long delays that could hang the system
        if policy.max_interval > Duration::from_secs(150) {
            policy.max_interval = Duration::from_secs(150);
        }

        Self { policy }
    }

    /// Executes an operation with retry logic and exponential backoff
    /// Generic over operation type to support any async function
    /// Respects cancellation tokens for graceful shutdown
    async fn execute_with_retry<F, Fut, T>(
        &self,
        ctx: CancellationToken,
        task_name: &str,
        mut operation: F,
    ) -> Result<T, Error>
    where
        F: FnMut(i32) -> Fut,
        Fut: std::future::Future<Output = Result<T, Error>>,
    {
        // If retries disabled, execute once
        if self.policy.max_attempts <= 0 {
            return operation(0).await;
        }

        let mut last_error = None;

        // Attempt execution up to max_attempts times
        for attempt in 1..=self.policy.max_attempts {
            // Check for cancellation before each attempt
            if ctx.is_cancelled() {
                return Err(Error::ContextCancelled(format!(
                    "Context cancelled during retry attempt {}",
                    attempt
                )));
            }

            match operation(attempt).await {
                Ok(result) => return Ok(result), // Success - return immediately
                Err(e) => last_error = Some(e),  // Store error for final report
            }

            // Wait before next attempt (except after last attempt)
            if attempt < self.policy.max_attempts {
                let wait_time = self.calculate_backoff(attempt);
                tokio::select! {
                    // Respect cancellation during wait
                    _ = ctx.cancelled() => {
                        return Err(Error::ContextCancelled(
                            "Context cancelled during retry wait".to_string()
                        ));
                    }
                    // Wait for calculated backoff duration
                    _ = sleep(wait_time) => {}
                }
            }
        }

        // All attempts failed - return comprehensive error
        Err(Error::RetryFailed(format!(
            "Task {} failed after {} attempts, last error: {:?}",
            task_name, self.policy.max_attempts, last_error
        )))
    }

    /// Calculates exponential backoff delay for given attempt number
    /// Applies multiplier and respects maximum interval limit
    fn calculate_backoff(&self, attempt: i32) -> Duration {
        let backoff = self.policy.interval.as_secs_f64() * self.policy.multiplier.powi(attempt - 1);
        let result = Duration::from_secs_f64(backoff);
        // Ensure we don't exceed maximum interval
        result.min(self.policy.max_interval)
    }
}

/// Core trait defining task behavior and lifecycle
/// All tasks must implement this trait to be executable by the DAG
/// Provides hooks for custom pre/post processing logic
#[async_trait]
pub trait Task: Send + Sync {
    /// Returns the unique name identifier for this task
    fn name(&self) -> &str;

    /// Returns list of task names this task depends on
    /// Used to build the dependency graph
    fn dependencies(&self) -> Vec<String>;

    /// Returns retry policy for this specific task
    /// None means use system defaults
    fn retry_policy(&self) -> Option<RetryPolicy>;

    /// Pre-execution hook called before main task execution
    /// Useful for setup, validation, or logging
    /// Default implementation does nothing
    async fn pre_execution(
        &self,
        _ctx: CancellationToken,
        _input: &TaskInput,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Main task execution logic - must be implemented by each task
    /// Receives input from dependencies and returns results
    /// This is the core business logic of the task
    async fn execute(&self, ctx: CancellationToken, input: &TaskInput)
    -> Result<TaskResult, Error>;

    /// Post-execution hook called after successful main execution
    /// Useful for cleanup, notifications, or result processing
    /// Default implementation does nothing
    async fn post_execution(
        &self,
        _ctx: CancellationToken,
        _output: &TaskResult,
    ) -> Result<(), Error> {
        Ok(())
    }
}

// Re-export the DAG executor for external use
pub use crate::executor::Dag;
