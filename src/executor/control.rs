use super::Dag;
use crate::control::TaskControl;
use crate::{CancelCause, CancelKind, CancelReason, Error};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;

impl Dag {
    fn with_pending_control<F>(&self, name: &str, command: F) -> Result<(), Error>
    where
        F: FnOnce(&Arc<TaskControl>),
    {
        let control = self
            .controls
            .get(name)
            .ok_or_else(|| Error::UnknownTask(name.into()))?;
        let run = self.run.lock().unwrap();
        if run.statuses[name].state.done() {
            return Err(Error::TaskAlreadyDone(name.into()));
        }
        command(control);
        Ok(())
    }

    pub fn cancel_task(&self, name: &str) -> Result<(), Error> {
        self.with_pending_control(name, |control| {
            control.cancel(CancelReason::new(CancelKind::Task, None));
        })
    }

    pub fn cancel_task_with_cause(&self, name: &str, cause: CancelCause) -> Result<(), Error> {
        self.with_pending_control(name, |control| {
            control.cancel(CancelReason::new(CancelKind::Task, Some(cause)));
        })
    }

    pub fn suspend_task(&self, name: &str) -> Result<(), Error> {
        self.with_pending_control(name, |control| control.set_task_suspended(true))
    }

    pub fn resume_task(&self, name: &str) -> Result<(), Error> {
        self.with_pending_control(name, |control| control.set_task_suspended(false))
    }

    pub fn suspended_task(&self, name: &str) -> bool {
        let run = self.run.lock().unwrap();
        let Some(control) = self.controls.get(name) else {
            return false;
        };
        !run.statuses[name].state.done() && control.suspended()
    }

    pub fn suspend(&self) {
        let mut run = self.run.lock().unwrap();
        run.run_suspended = true;
        for control in self.controls.values() {
            control.set_run_suspended(true);
        }
    }

    pub fn resume(&self) {
        let mut run = self.run.lock().unwrap();
        run.run_suspended = false;
        for control in self.controls.values() {
            control.set_run_suspended(false);
        }
    }

    pub fn suspended(&self) -> bool {
        self.run.lock().unwrap().run_suspended
    }

    pub fn canceled(&self) -> bool {
        self.cancel_requested.load(Ordering::Acquire)
    }

    pub async fn cancel(&self, grace: Duration) -> Result<(), Error> {
        self.cancel_with_reason(grace, CancelReason::new(CancelKind::Run, None))
            .await
    }

    pub async fn cancel_with_cause(
        &self,
        grace: Duration,
        cause: CancelCause,
    ) -> Result<(), Error> {
        self.cancel_with_reason(grace, CancelReason::new(CancelKind::Run, Some(cause)))
            .await
    }

    async fn cancel_with_reason(&self, grace: Duration, reason: CancelReason) -> Result<(), Error> {
        let start_guard = self.start_gate.lock().unwrap();
        let first = {
            let mut stored = self.cancel_reason.lock().unwrap();
            if stored.is_some() {
                false
            } else {
                *stored = Some(reason.clone());
                true
            }
        };
        if first {
            self.cancel_requested.store(true, Ordering::Release);
            for control in self.controls.values() {
                control.cancel(reason.clone());
            }
        }
        let started = self.started.load(Ordering::Acquire);
        drop(start_guard);
        if !started {
            return Ok(());
        }

        let mut done = self.done.subscribe();
        if *done.borrow_and_update() {
            return Ok(());
        }
        let mut watchdog_done = done.clone();
        let force = self.force.clone();
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = wait_done(&mut watchdog_done) => {}
                _ = sleep(grace) => {
                    if !*watchdog_done.borrow() {
                        force.send_replace(true);
                    }
                }
            }
        });

        tokio::select! {
            biased;
            _ = wait_done(&mut done) => Ok(()),
            _ = sleep(grace) => {
                self.force.send_replace(true);
                wait_done(&mut done).await;
                Err(Error::GracePeriodExpired)
            }
        }
    }
}

async fn wait_done(done: &mut watch::Receiver<bool>) {
    while !*done.borrow_and_update() {
        if done.changed().await.is_err() {
            return;
        }
    }
}
