use super::Dag;
use crate::{Progress, RunPhase, TaskState, TaskStatus};
use std::collections::HashMap;

impl Dag {
    pub fn phase(&self) -> RunPhase {
        self.run.lock().unwrap().phase
    }

    pub fn states(&self) -> HashMap<String, TaskState> {
        self.run
            .lock()
            .unwrap()
            .statuses
            .iter()
            .map(|(name, status)| (name.clone(), status.state))
            .collect()
    }

    pub fn state(&self, name: &str) -> Option<TaskState> {
        self.run.lock().unwrap().statuses.get(name).map(|s| s.state)
    }

    pub fn task_results(&self) -> HashMap<String, TaskStatus> {
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
                TaskState::Pending => progress.pending += 1,
                TaskState::Success => progress.success += 1,
                TaskState::Skipped => progress.skipped += 1,
                TaskState::Canceled => progress.canceled += 1,
                TaskState::Failed => progress.failed += 1,
            }
        }
        progress
    }
}
