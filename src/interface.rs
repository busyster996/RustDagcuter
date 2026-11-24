use crate::{
    CancelOption, Context, Error, Outputs, Phase, Progress, RunError, Scheduler, State, TaskResult,
};
use async_trait::async_trait;
use std::{collections::HashMap, io};

/// Control an execution without granting the ability to start it.
#[async_trait]
pub trait Control: Send + Sync {
    async fn cancel(&self, grace: Context, options: &[CancelOption]) -> Result<(), Error>;
    fn cancel_task(&self, name: &str, options: &[CancelOption]) -> Result<(), Error>;
    fn canceled(&self) -> bool;
    fn suspend(&self);
    fn resume(&self);
    fn suspend_task(&self, name: &str) -> Result<(), Error>;
    fn resume_task(&self, name: &str) -> Result<(), Error>;
    fn suspended(&self) -> bool;
    fn suspended_task(&self, name: &str) -> bool;
}

/// Start, control, and inspect a scheduler. This trait is object-safe.
#[async_trait]
pub trait SchedulerApi: Control {
    async fn execute(&self, ctx: Context) -> (Outputs, Option<RunError>);
    fn phase(&self) -> Phase;
    fn states(&self) -> HashMap<String, State>;
    fn state(&self, name: &str) -> State;
    fn results(&self) -> HashMap<String, TaskResult>;
    fn progress(&self) -> Progress;
    fn success_completion_order(&self) -> String;
    fn print_graph(&self);
    fn write_graph(&self, writer: &mut dyn io::Write) -> io::Result<()>;
}

#[async_trait]
impl Control for Scheduler {
    async fn cancel(&self, grace: Context, options: &[CancelOption]) -> Result<(), Error> {
        Scheduler::cancel(self, grace, options).await
    }

    fn cancel_task(&self, name: &str, options: &[CancelOption]) -> Result<(), Error> {
        Scheduler::cancel_task(self, name, options)
    }

    fn canceled(&self) -> bool {
        Scheduler::canceled(self)
    }
    fn suspend(&self) {
        Scheduler::suspend(self)
    }
    fn resume(&self) {
        Scheduler::resume(self)
    }
    fn suspend_task(&self, name: &str) -> Result<(), Error> {
        Scheduler::suspend_task(self, name)
    }
    fn resume_task(&self, name: &str) -> Result<(), Error> {
        Scheduler::resume_task(self, name)
    }
    fn suspended(&self) -> bool {
        Scheduler::suspended(self)
    }
    fn suspended_task(&self, name: &str) -> bool {
        Scheduler::suspended_task(self, name)
    }
}

#[async_trait]
impl SchedulerApi for Scheduler {
    async fn execute(&self, ctx: Context) -> (Outputs, Option<RunError>) {
        Scheduler::execute(self, ctx).await
    }

    fn phase(&self) -> Phase {
        Scheduler::phase(self)
    }
    fn states(&self) -> HashMap<String, State> {
        Scheduler::states(self)
    }
    fn state(&self, name: &str) -> State {
        Scheduler::state(self, name)
    }
    fn results(&self) -> HashMap<String, TaskResult> {
        Scheduler::results(self)
    }
    fn progress(&self) -> Progress {
        Scheduler::progress(self)
    }
    fn success_completion_order(&self) -> String {
        Scheduler::success_completion_order(self)
    }
    fn print_graph(&self) {
        Scheduler::print_graph(self)
    }
    fn write_graph(&self, writer: &mut dyn io::Write) -> io::Result<()> {
        Scheduler::write_graph(self, writer)
    }
}
