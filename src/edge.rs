use crate::{Data, Error, State};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A case-sensitive edge label. The empty label is equivalent to [`ON_SUCCESS`].
pub type Trigger = String;

pub const ON_SUCCESS: &str = "success";
pub const ON_FAILURE: &str = "failure";

/// An incoming edge from `task`, activated by the given label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyEdge {
    pub task: String,
    pub on: Trigger,
}

impl DependencyEdge {
    pub fn new(task: impl Into<String>, on: impl Into<Trigger>) -> Self {
        Self {
            task: task.into(),
            on: on.into(),
        }
    }
}

/// The join is evaluated once all direct upstream tasks have settled.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum JoinPolicy {
    /// Every incoming edge must be active.
    #[default]
    AllOf,
    /// At least one incoming edge must be active.
    AnyOf,
}

/// Incoming edges and join policy. Repeated edges count individually.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DependencySpec {
    pub edges: Vec<DependencyEdge>,
    pub join: JoinPolicy,
}

/// Run after all of these tasks succeed and emit the default success label.
pub fn after(tasks: impl IntoIterator<Item = impl Into<String>>) -> DependencySpec {
    DependencySpec {
        edges: tasks
            .into_iter()
            .map(|task| DependencyEdge::new(task, ON_SUCCESS))
            .collect(),
        join: JoinPolicy::AllOf,
    }
}

/// Input from active edges only, keyed by upstream task name.
#[derive(Debug, Clone, Default)]
pub struct Input {
    pub outputs: HashMap<String, Data>,
    pub errors: HashMap<String, Arc<Error>>,
}

/// Output data and the labels to activate when the task succeeds.
#[derive(Debug, Clone, Default)]
pub struct Output {
    pub data: Data,
    /// Empty means `[ON_SUCCESS]`; otherwise only the listed labels are emitted.
    /// [`ON_FAILURE`] is reserved and cannot be emitted explicitly.
    pub emit: Vec<Trigger>,
}

impl From<Data> for Output {
    fn from(data: Data) -> Self {
        Self {
            data,
            emit: Vec::new(),
        }
    }
}

impl Output {
    pub(crate) fn into_parts(self) -> (Data, HashSet<Trigger>) {
        let mut labels: HashSet<_> = self.emit.into_iter().map(normalize).collect();
        if labels.is_empty() {
            labels.insert(ON_SUCCESS.into());
        }
        (self.data, labels)
    }
}

pub(crate) fn normalize(mut trigger: Trigger) -> Trigger {
    if trigger.is_empty() {
        trigger.push_str(ON_SUCCESS);
    }
    trigger
}

pub(crate) fn edge_is_active(
    state: State,
    emitted: Option<&HashSet<Trigger>>,
    trigger: &str,
) -> bool {
    match state {
        State::Success => emitted.is_some_and(|labels| labels.contains(trigger)),
        State::Failed => trigger == ON_FAILURE,
        _ => false,
    }
}
