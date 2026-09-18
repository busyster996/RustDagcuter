use crate::control::TaskControl;
use crate::{Error, RetryPolicy, TaskResult};
use rand::RngExt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

pub(crate) struct Retry {
    policy: RetryPolicy,
}

impl Retry {
    pub(crate) fn new(policy: Option<RetryPolicy>) -> Self {
        let mut policy = policy.unwrap_or_default();
        if policy.max_attempts == 0 {
            policy.max_attempts = 1;
        }
        if policy.interval.is_zero() {
            policy.interval = Duration::from_secs(1);
        }
        if policy.max_interval.is_zero() {
            policy.max_interval = Duration::from_secs(30);
        }
        policy.max_interval = policy.max_interval.min(Duration::from_secs(150));
        if !policy.multiplier.is_finite() || policy.multiplier <= 0.0 {
            policy.multiplier = 2.0;
        }
        if policy.jitter.is_nan() || policy.jitter < 0.0 {
            policy.jitter = 0.0;
        }
        policy.jitter = policy.jitter.min(1.0);
        Self { policy }
    }

    fn delay(&self, attempt: u64) -> Duration {
        let sample = if self.policy.jitter > 0.0 {
            rand::rng().random::<f64>()
        } else {
            0.0
        };
        self.delay_with_sample(attempt, sample)
    }

    fn delay_with_sample(&self, attempt: u64, sample: f64) -> Duration {
        let seconds = self.policy.interval.as_secs_f64()
            * self
                .policy
                .multiplier
                .powf(attempt.saturating_sub(1) as f64);
        let maximum = self.policy.max_interval.as_secs_f64();
        let capped = if seconds.is_nan() || seconds >= maximum {
            maximum
        } else if seconds <= 0.0 {
            0.0
        } else {
            seconds
        };
        Duration::from_secs_f64(capped * (1.0 - self.policy.jitter * sample))
    }

    pub(crate) async fn run<F, Fut>(
        &self,
        ctx: &CancellationToken,
        semaphore: Option<Arc<Semaphore>>,
        control: Option<Arc<TaskControl>>,
        mut operation: F,
    ) -> (Result<TaskResult, Error>, u64)
    where
        F: FnMut(u64) -> Fut,
        Fut: Future<Output = Result<TaskResult, Error>>,
    {
        let mut attempt = 0u64;
        let mut last_error = None;
        loop {
            if ctx.is_cancelled() {
                return (
                    Err(interrupted(last_error, "canceled before attempt")),
                    attempt,
                );
            }
            if let Some(control) = &control {
                if let Err(error) = control.wait(ctx).await {
                    return (
                        Err(if ctx.is_cancelled() {
                            interrupted(last_error, "canceled while suspended")
                        } else {
                            error
                        }),
                        attempt,
                    );
                }
            }
            let permit = if let Some(semaphore) = &semaphore {
                tokio::select! {
                    _ = ctx.cancelled() => {
                        return (Err(interrupted(last_error, "canceled while waiting for concurrency slot")), attempt);
                    }
                    result = semaphore.clone().acquire_owned() => {
                        match result {
                            Ok(permit) => Some(permit),
                            Err(_) => return (Err(Error::TaskExecution("concurrency semaphore closed".into())), attempt),
                        }
                    }
                }
            } else {
                None
            };
            if ctx.is_cancelled() {
                drop(permit);
                return (
                    Err(interrupted(last_error, "canceled before attempt")),
                    attempt,
                );
            }
            attempt = attempt.saturating_add(1);
            let result = operation(attempt).await;
            drop(permit);
            match result {
                Ok(output) => return (Ok(output), attempt),
                Err(error) => {
                    if ctx.is_cancelled() && matches!(error, Error::ContextCancelled(_)) {
                        return (Err(error), attempt);
                    }
                    if error.is_non_retryable() {
                        return (Err(error), attempt);
                    }
                    if self.policy.max_attempts > 0 && attempt >= self.policy.max_attempts as u64 {
                        return (
                            Err(Error::RetryFailed {
                                last: Box::new(error),
                            }),
                            attempt,
                        );
                    }
                    last_error = Some(error);
                    tokio::select! {
                        _ = ctx.cancelled() => {
                            return (Err(interrupted(last_error, "canceled during retry wait")), attempt);
                        }
                        _ = sleep(self.delay(attempt)) => {}
                    }
                }
            }
        }
    }
}

fn interrupted(last_error: Option<Error>, where_: &str) -> Error {
    match last_error {
        Some(last) => Error::RetryInterrupted {
            last: Box::new(last),
        },
        None => Error::ContextCancelled(where_.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::panic::AssertUnwindSafe;

    #[test]
    fn jitter_is_bounded_after_backoff_is_capped() {
        let retry = Retry::new(Some(RetryPolicy {
            interval: Duration::from_secs(100),
            max_interval: Duration::from_secs(1000),
            max_attempts: 3,
            multiplier: 10.0,
            jitter: 0.5,
        }));
        assert_eq!(retry.policy.max_interval, Duration::from_secs(150));
        assert_eq!(retry.delay_with_sample(2, 0.0), Duration::from_secs(150));
        assert_eq!(retry.delay_with_sample(2, 1.0), Duration::from_secs(75));
        assert_eq!(
            retry.delay_with_sample(2, 0.5),
            Duration::from_millis(112_500)
        );
    }

    #[test]
    fn invalid_floats_normalize_without_destroying_valid_subunit_multiplier() {
        let invalid = Retry::new(Some(RetryPolicy {
            multiplier: f64::NAN,
            jitter: f64::NAN,
            ..RetryPolicy::default()
        }));
        assert_eq!(invalid.policy.multiplier, 2.0);
        assert_eq!(invalid.policy.jitter, 0.0);

        let shrinking = Retry::new(Some(RetryPolicy {
            interval: Duration::from_secs(10),
            multiplier: 0.5,
            jitter: -1.0,
            ..RetryPolicy::default()
        }));
        assert_eq!(shrinking.delay_with_sample(2, 0.5), Duration::from_secs(5));
        assert_eq!(shrinking.delay_with_sample(10_000, 0.5), Duration::ZERO);

        let capped = Retry::new(Some(RetryPolicy {
            interval: Duration::from_secs(1),
            multiplier: f64::INFINITY,
            jitter: f64::INFINITY,
            ..RetryPolicy::default()
        }));
        assert_eq!(capped.policy.multiplier, 2.0);
        assert_eq!(capped.policy.jitter, 1.0);
        assert_eq!(
            capped.delay_with_sample(10_000, 0.0),
            Duration::from_secs(30)
        );
    }

    #[tokio::test]
    async fn retry_backoff_releases_the_concurrency_permit() {
        let semaphore = Arc::new(Semaphore::new(1));
        let token = CancellationToken::new();
        let retry = Retry::new(Some(RetryPolicy {
            interval: Duration::from_secs(30),
            max_attempts: 2,
            ..RetryPolicy::default()
        }));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut started_tx = Some(started_tx);
        let worker_token = token.clone();
        let worker_semaphore = semaphore.clone();
        let worker = tokio::spawn(async move {
            retry
                .run(&worker_token, Some(worker_semaphore), None, |_| {
                    if let Some(sender) = started_tx.take() {
                        let _ = sender.send(());
                    }
                    async { Err(Error::TaskExecution("transient".into())) }
                })
                .await
        });
        started_rx.await.unwrap();
        let permit = tokio::time::timeout(Duration::from_secs(2), semaphore.acquire_owned())
            .await
            .expect("retry must release its slot before backoff")
            .unwrap();
        token.cancel();
        drop(permit);
        let (result, attempts) = worker.await.unwrap();
        assert!(matches!(result, Err(Error::RetryInterrupted { .. })));
        assert_eq!(attempts, 1);
    }

    #[tokio::test]
    async fn panicking_attempt_does_not_leak_the_concurrency_permit() {
        let semaphore = Arc::new(Semaphore::new(1));
        let retry = Retry::new(None);
        let token = CancellationToken::new();
        let result =
            AssertUnwindSafe(retry.run(&token, Some(semaphore.clone()), None, |_| async {
                panic!("attempt panicked");
                #[allow(unreachable_code)]
                Ok(TaskResult::new())
            }))
            .catch_unwind()
            .await;
        assert!(result.is_err());
        let _permit = tokio::time::timeout(Duration::from_secs(2), semaphore.acquire_owned())
            .await
            .expect("panic must release its slot")
            .unwrap();
    }
}
