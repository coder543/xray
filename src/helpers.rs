use std::time::Duration;

pub trait ReadableDuration {
    fn readable(&self) -> String;
}

impl ReadableDuration for Duration {
    fn readable(&self) -> String {
        let total = self.as_secs() as f64 + (self.subsec_nanos() as f64) / 1_000_000_000.0;
        if total < 0.000001 {
            format!("{} ns", total * 1000.0 * 1000.0 * 1000.0)
        } else if total < 0.001 {
            format!("{} us", total * 1000.0 * 1000.0)
        } else if total < 1.0 {
            format!("{} ms", total * 1000.0)
        } else {
            format!("{} secs", total)
        }
    }
}
