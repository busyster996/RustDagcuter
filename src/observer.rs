use crate::{Error, TaskStatus};
use std::any::Any;
use std::backtrace::Backtrace;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Event {
    pub task: String,
    pub status: TaskStatus,
}

#[derive(Debug, Clone)]
pub struct ObserverFailure {
    pub task: String,
    pub error: Arc<Error>,
}

pub(crate) fn notify(observer: &(dyn Fn(Event) + Send + Sync), event: Event) -> Result<(), Error> {
    let task = event.task.clone();
    catch_unwind(AssertUnwindSafe(|| observer(event))).map_err(|payload| Error::ObserverPanic {
        task,
        message: panic_message(&*payload),
        stack: Backtrace::force_capture().to_string(),
    })
}

pub(crate) fn panic_message(payload: &dyn Any) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".into()
    }
}
