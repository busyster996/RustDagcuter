use crate::Event;
use std::sync::Arc;

pub(crate) type Observer = Arc<dyn Fn(Event) + Send + Sync>;

#[derive(Default)]
pub(crate) struct Options {
    pub(crate) max_concurrency: isize,
    pub(crate) observer: Option<Observer>,
}

#[derive(Clone)]
pub struct SchedulerOption(OptionKind);

#[derive(Clone)]
enum OptionKind {
    MaxConcurrency(isize),
    Observer(Observer),
}

impl SchedulerOption {
    pub(crate) fn apply(self, options: &mut Options) {
        match self.0 {
            OptionKind::MaxConcurrency(limit) => options.max_concurrency = limit,
            OptionKind::Observer(observer) => options.observer = Some(observer),
        }
    }
}

/// Set the maximum number of simultaneous attempts. Nonpositive values are unlimited.
pub fn with_max_concurrency(limit: isize) -> SchedulerOption {
    SchedulerOption(OptionKind::MaxConcurrency(limit))
}

/// Observe terminal task results. Callbacks may run concurrently.
pub fn with_observer(observer: impl Fn(Event) + Send + Sync + 'static) -> SchedulerOption {
    SchedulerOption(OptionKind::Observer(Arc::new(observer)))
}
