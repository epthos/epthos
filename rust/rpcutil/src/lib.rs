use std::time::Duration;

pub mod auth;
pub mod testing;

/// Add random jitter to a base duration. The final duration is longer
/// than the initial one.
pub fn jitter(base: Duration, percent: u8) -> Duration {
    let base = base.as_millis() as u64;
    let jitter = if base > 0 {
        (rand::random::<u64>() % base) / percent as u64
    } else {
        0
    };

    Duration::from_millis(base + jitter)
}

/// Exponential backoff parameters.
pub struct Backoff {
    /// The initial delay.
    pub min_delay: Duration,
    /// The maximum delay.
    pub max_delay: Duration,
}

/// Exponential backoff implementation.
#[derive(Clone)]
pub struct ExpBackoff {
    now: u64,
    min: u64,
    max: u64,
}

impl ExpBackoff {
    /// Defines the exponential backoff ranges, from |min| to |max|.
    pub fn new(config: &Backoff) -> ExpBackoff {
        ExpBackoff {
            now: config.min_delay.as_millis() as u64,
            min: config.min_delay.as_millis() as u64,
            max: config.max_delay.as_millis() as u64,
        }
    }
    /// Returns the next backoff delay.
    pub fn again(&mut self) -> Duration {
        let now = Duration::from_millis(self.now);
        self.now = self.max.min(self.now * 2);
        now
    }

    pub fn reset(&mut self) {
        self.now = self.min;
    }
}

/// A sleep function which adds 10% jitter to the duration.
///
/// This is the recommended way to sleep, as it avoids unexpected
/// synchronizations and can be tested using tokio::time::pause().
pub async fn jittery_sleep(duration: Duration) {
    tokio::time::sleep(jitter(duration, 10)).await;
}

/// Does async sleep.
#[tonic::async_trait]
pub trait Sleeper: Send + Sync {
    async fn sleep(&self, duration: Duration);
}

/// A Sleeper which adds some jitter to the duration.
pub fn jittery_sleeper(jitter: u8) -> Box<dyn Sleeper> {
    Box::new(JitterySleep { jitter })
}

struct JitterySleep {
    jitter: u8,
}

#[tonic::async_trait]
impl Sleeper for JitterySleep {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(jitter(duration, self.jitter)).await;
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;

    #[test]
    fn jitter_range() {
        let mut total = Duration::ZERO;
        for _ in 0..10 {
            total += jitter(Duration::from_secs(1), 10);
        }
        assert!(total.as_millis() >= 10000);
        assert!(total.as_millis() <= 11000);
    }

    #[test]
    fn backoff_range() {
        let mut backoff = ExpBackoff::new(&Backoff {
            min_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(10),
        });

        let delays = vec![
            backoff.again(),
            backoff.again(),
            backoff.again(),
            backoff.again(),
            backoff.again(),
        ];
        assert_eq!(
            delays,
            vec![
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                Duration::from_secs(10),
                Duration::from_secs(10),
            ]
        )
    }
}
