use super::Scheduler;
use crate::{Phase, Progress, State, TaskResult};
use std::collections::HashMap;

impl Scheduler {
    pub fn phase(&self) -> Phase {
        self.run.lock().unwrap().phase
    }

    pub fn states(&self) -> HashMap<String, State> {
        self.run
            .lock()
            .unwrap()
            .statuses
            .iter()
            .map(|(name, status)| (name.clone(), status.state))
            .collect()
    }

    pub fn state(&self, name: &str) -> State {
        self.run
            .lock()
            .unwrap()
            .statuses
            .get(name)
            .map_or(State::Pending, |s| s.state)
    }

    pub fn results(&self) -> HashMap<String, TaskResult> {
        self.run.lock().unwrap().statuses.clone()
    }

    pub fn progress(&self) -> Progress {
        let run = self.run.lock().unwrap();
        let mut progress = Progress {
            total: run.statuses.len(),
            ..Progress::default()
        };
        for status in run.statuses.values() {
            match status.state {
                State::Pending => progress.pending += 1,
                State::Success => progress.success += 1,
                State::Skipped => progress.skipped += 1,
                State::Canceled => progress.canceled += 1,
                State::Failed => progress.failed += 1,
            }
        }
        progress
    }
}
