use std::fmt;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    pub total: usize,
    pub pending: usize,
    pub success: usize,
    pub skipped: usize,
    pub canceled: usize,
    pub failed: usize,
}

impl Progress {
    pub fn done(self) -> usize {
        self.total - self.pending
    }

    pub fn ratio(self) -> f64 {
        if self.total == 0 {
            1.0
        } else {
            self.done() as f64 / self.total as f64
        }
    }
}

impl fmt::Display for Progress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{} done (success {}, skipped {}, canceled {}, failed {})",
            self.done(),
            self.total,
            self.success,
            self.skipped,
            self.canceled,
            self.failed
        )
    }
}
