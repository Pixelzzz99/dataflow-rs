use crate::error::EtlError;
use tokio::time::{Duration, sleep};

pub async fn retry_with_backoff<F, Fut, T>(
    max_attemts: u32,
    base_delay_secs: u64,
    operation_name: &str,
    f: F,
) -> Result<T, EtlError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, EtlError>>,
{
    let mut last_error = None;

    for attempt in 1..=max_attemts {
        match f().await {
            Ok(result) => {
                if attempt > 1 {
                    log::info!("{}: succeeded on attempt {}", operation_name, attempt);
                }
                return Ok(result);
            }
            Err(e) => {
                log::warn!(
                    "{}: attempt {}/{} failed: {}",
                    operation_name,
                    attempt,
                    max_attemts,
                    e
                );
                last_error = Some(e);

                if attempt < max_attemts {
                    let delay = base_delay_secs * 2u64.pow(attempt - 1);
                    let capped = delay.min(60); // Cap the delay to 60 seconds
                    log::info!("{}: retrying in {} seconds...", operation_name, capped);
                    sleep(Duration::from_secs(capped)).await;
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        EtlError::ConnectionError(format!(
            "{}: all {} attempts failed",
            operation_name, max_attemts
        ))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn test_succeeds_on_first_attempt() {
        let result = retry_with_backoff(3, 0, "test", || async { Ok::<i32, EtlError>(42) }).await;

        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_succeeds_after_failures() {
        let attempts = Arc::new(Mutex::new(0));
        let attemtps_clone = attempts.clone();

        let result = retry_with_backoff(3, 0, "test", move || {
            let counter = attemtps_clone.clone();

            async move {
                let mut n = counter.lock().unwrap();
                *n += 1;
                if *n < 3 {
                    Err(EtlError::ConnectionError("not yet".to_string()))
                } else {
                    Ok(99)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 99);
        assert_eq!(*attempts.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn test_fails_after_max_attempts() {
        let result = retry_with_backoff(3, 0, "test", || async {
            Err::<i32, EtlError>(EtlError::ConnectionError("always fails".to_string()))
        })
        .await;

        assert!(result.is_err());
    }
}
