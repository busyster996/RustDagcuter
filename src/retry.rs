use crate::control::TaskControl;
use crate::{Error, RetryPolicy};
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
    pub(crate) fn new(mut policy: RetryPolicy) -> Self {
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

    fn delay(&self, attempt: i64) -> Duration {
        let sample = if self.policy.jitter > 0.0 {
            rand::rng().random::<f64>()
        } else {
            0.0
        };
        self.delay_with_sample(attempt, sample)
    }

    fn delay_with_sample(&self, attempt: i64, sample: f64) -> Duration {
        // Go's time.Duration stores integer nanoseconds and truncates
        // both the exponential backoff and the subsequent jitter.
        let maximum = self.policy.max_interval.as_nanos() as u64;
        let nanos = self.policy.interval.as_nanos() as f64
            * self.policy.multiplier.powf((attempt - 1) as f64);
        let capped = if nanos.is_nan() || nanos >= maximum as f64 {
            maximum
        } else {
            nanos as u64
        };
        Duration::from_nanos(if self.policy.jitter > 0.0 && capped > 0 {
            (capped as f64 * (1.0 - self.policy.jitter * sample)) as u64
        } else {
            capped
        })
    }

    pub(crate) async fn run<T, F, Fut>(
        &self,
        ctx: &CancellationToken,
        semaphore: Option<Arc<Semaphore>>,
        control: Option<Arc<TaskControl>>,
        mut operation: F,
    ) -> (Result<T, Error>, i64)
    where
        F: FnMut(i64) -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        let mut attempt = 0i64;
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
                    let error = if error.is_interruption() {
                        last_error.take().unwrap_or(error)
                    } else {
                        error
                    };
                    if error.is_non_retryable() {
                        return (Err(error), attempt);
                    }
                    if self.policy.max_attempts > 0 && attempt >= self.policy.max_attempts {
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
    fn backoff_and_jitter_each_truncate_fractional_nanoseconds() {
        let retry = Retry::new(RetryPolicy {
            interval: Duration::from_nanos(3),
            multiplier: 0.6,
            jitter: 1.0,
            ..RetryPolicy::default()
        });
        assert_eq!(retry.delay_with_sample(2, 0.0), Duration::from_nanos(1));
        assert_eq!(retry.delay_with_sample(2, 0.25), Duration::ZERO);
    }

    #[test]
    fn jitter_is_bounded_after_backoff_is_capped() {
        let retry = Retry::new(RetryPolicy {
            interval: Duration::from_secs(100),
            max_interval: Duration::from_secs(1000),
            max_attempts: 3,
            multiplier: 10.0,
            jitter: 0.5,
        });
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
        let invalid = Retry::new(RetryPolicy {
            multiplier: f64::NAN,
            jitter: f64::NAN,
            ..RetryPolicy::default()
        });
        assert_eq!(invalid.policy.multiplier, 2.0);
        assert_eq!(invalid.policy.jitter, 0.0);

        let shrinking = Retry::new(RetryPolicy {
            interval: Duration::from_secs(10),
            multiplier: 0.5,
            jitter: -1.0,
            ..RetryPolicy::default()
        });
        assert_eq!(shrinking.delay_with_sample(2, 0.5), Duration::from_secs(5));
        assert_eq!(shrinking.delay_with_sample(10_000, 0.5), Duration::ZERO);

        let capped = Retry::new(RetryPolicy {
            interval: Duration::from_secs(1),
            multiplier: f64::INFINITY,
            jitter: f64::INFINITY,
            ..RetryPolicy::default()
        });
        assert_eq!(capped.policy.multiplier, 2.0);
        assert_eq!(capped.policy.jitter, 1.0);
        assert_eq!(
            capped.delay_with_sample(10_000, 0.0),
            Duration::from_secs(30)
        );
    }

    #[tokio::test]
    async fn zero_attempts_means_one_attempt_even_with_custom_interval() {
        let zero = RetryPolicy {
            interval: Duration::ZERO,
            max_interval: Duration::ZERO,
            max_attempts: 0,
            multiplier: 0.0,
            jitter: 0.0,
        };
        for policy in [
            RetryPolicy::default(),
            zero.clone(),
            RetryPolicy {
                interval: Duration::from_millis(1),
                ..zero
            },
        ] {
            let retry = Retry::new(policy);
            let (result, attempts) = retry
                .run(&CancellationToken::new(), None, None, |_| async {
                    Err::<(), _>(Error::TaskExecution("failed".into()))
                })
                .await;
            assert!(matches!(result, Err(Error::RetryFailed { .. })));
            assert_eq!(attempts, 1);
        }
    }

    #[tokio::test]
    async fn retry_backoff_releases_the_concurrency_permit() {
        let semaphore = Arc::new(Semaphore::new(1));
        let token = CancellationToken::new();
        let retry = Retry::new(RetryPolicy {
            interval: Duration::from_secs(30),
            max_attempts: 2,
            ..RetryPolicy::default()
        });
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
                    async { Err::<(), _>(Error::TaskExecution("transient".into())) }
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
        let retry = Retry::new(RetryPolicy::default());
        let token = CancellationToken::new();
        let result =
            AssertUnwindSafe(retry.run(&token, Some(semaphore.clone()), None, |_| async {
                panic!("attempt panicked");
                #[allow(unreachable_code)]
                Ok(())
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
