use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use crate::DagcuterError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub interval: Duration,
    pub max_interval: Duration,
    pub max_attempts: i32,
    pub multiplier: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(30),
            max_attempts: -1,
            multiplier: 2.0,
        }
    }
}

pub struct RetryExecutor {
    policy: RetryPolicy,
}

impl RetryExecutor {
    pub fn new(policy: Option<RetryPolicy>) -> Self {
        let mut policy = policy.unwrap_or_default();

        // 设置默认值
        if policy.interval.is_zero() {
            policy.interval = Duration::from_secs(1);
        }
        if policy.max_interval.is_zero() {
            policy.max_interval = Duration::from_secs(30);
        }
        if policy.multiplier <= 0.0 {
            policy.multiplier = 2.0;
        }
        if policy.max_interval > Duration::from_secs(150) {
            policy.max_interval = Duration::from_secs(150);
        }

        Self { policy }
    }

    // 修复版本：让闭包直接返回结果，而不是通过外部变量
    pub async fn execute_with_retry<F, Fut, T>(
        &self,
        ctx: CancellationToken,
        task_name: &str,
        mut operation: F,
    ) -> Result<T, DagcuterError>
    where
        F: FnMut(i32) -> Fut,
        Fut: std::future::Future<Output = Result<T, DagcuterError>>,
    {
        if self.policy.max_attempts <= 0 {
            return operation(0).await;
        }

        let mut last_error = None;

        for attempt in 1..=self.policy.max_attempts {
            if ctx.is_cancelled() {
                return Err(DagcuterError::ContextCancelled(
                    format!("Context cancelled during retry attempt {}", attempt)
                ));
            }

            match operation(attempt).await {
                Ok(result) => return Ok(result), // 直接返回结果
                Err(e) => last_error = Some(e),
            }

            if attempt == self.policy.max_attempts {
                break;
            }

            let wait_time = self.calculate_backoff(attempt);

            tokio::select! {
                _ = ctx.cancelled() => {
                    return Err(DagcuterError::ContextCancelled(
                        "Context cancelled during retry wait".to_string()
                    ));
                }
                _ = sleep(wait_time) => {
                    // 继续重试
                }
            }
        }

        Err(DagcuterError::RetryFailed(format!(
            "Task {} failed after {} attempts, last error: {:?}",
            task_name, self.policy.max_attempts, last_error
        )))
    }

    fn calculate_backoff(&self, attempt: i32) -> Duration {
        let backoff = self.policy.interval.as_secs_f64()
            * self.policy.multiplier.powi(attempt - 1);

        let result = Duration::from_secs_f64(backoff);

        if result > self.policy.max_interval {
            self.policy.max_interval
        } else {
            result
        }
    }
}

