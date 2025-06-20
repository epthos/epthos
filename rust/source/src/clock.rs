//! Clock abstracts time-based dependencies for testing.

use std::time::{Duration, SystemTime};

pub trait Clock {
    fn now(&self) -> SystemTime;
    fn sleep(&self, delay: Duration, id: &str) -> impl Future<Output = ()> + '_;
}

pub fn new() -> SystemClock {
    SystemClock {}
}

pub struct SystemClock {}

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    fn sleep(&self, delay: Duration, _: &str) -> impl Future<Output = ()> + '_ {
        tokio::time::sleep(delay)
    }
}
