use crate::Event;
use std::sync::Arc;

pub(crate) type Observer = Arc<dyn Fn(Event) + Send + Sync>;

#[derive(Default, Clone)]
pub struct DagOptions {
    pub(crate) max_concurrency: isize,
    pub(crate) observer: Option<Observer>,
}

impl DagOptions {
    pub fn max_concurrency(mut self, limit: isize) -> Self {
        self.max_concurrency = limit;
        self
    }

    pub fn observer<F>(mut self, callback: F) -> Self
    where
        F: Fn(Event) + Send + Sync + 'static,
    {
        self.observer = Some(Arc::new(callback));
        self
    }
}
