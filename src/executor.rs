mod control;
mod graph;
mod query;

use crate::control::TaskControl;
use crate::edge::{edge_is_active, normalize};
use crate::options::{Observer, Options};
use crate::retry::Retry;
use crate::{
    BoxTask, CancelKind, CancelReason, Context, Data, DependencySpec, Error, Event, Input,
    JoinPolicy, ObserverFailure, Output, Phase, RunError, SchedulerOption, State, TaskContext,
    TaskFailure, TaskResult, Trigger, ON_FAILURE,
};
use futures::future::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{watch, Semaphore};
use tokio::task::{AbortHandle, JoinHandle};
use tokio_util::sync::CancellationToken;

struct RunState {
    phase: Phase,
    statuses: HashMap<String, TaskResult>,
    outputs: HashMap<String, Data>,
    emitted: HashMap<String, HashSet<Trigger>>,
    order: Vec<String>,
    cancel_reported: bool,
    run_suspended: bool,
    degrees: HashMap<String, usize>,
}

struct Completion {
    state: State,
    output: Option<Output>,
    error: Option<Error>,
    attempts: i64,
}

impl Completion {
    fn failed(error: Error, attempts: i64) -> Self {
        Self {
            state: State::Failed,
            output: None,
            error: Some(error),
            attempts,
        }
    }
}

fn cancellation_error(error: Option<Error>, reported: &mut bool) -> Option<Error> {
    match error {
        Some(error) if !*reported => {
            *reported = true;
            Some(error)
        }
        Some(Error::RetryInterrupted { last } | Error::DeadlineInterrupted { last }) => Some(*last),
        Some(Error::RunCanceled {
            last: Some(last), ..
        }) => match *last {
            Error::RetryInterrupted { last } => Some(*last),
            last if !last.is_context_error() => Some(last),
            _ => None,
        },
        _ => None,
    }
}

struct OutgoingEdge {
    to: String,
    on: Trigger,
}

pub struct Scheduler {
    tasks: HashMap<String, BoxTask>,
    dependencies: HashMap<String, DependencySpec>,
    dependents: HashMap<String, Vec<OutgoingEdge>>,
    in_degrees: HashMap<String, usize>,
    controls: HashMap<String, Arc<TaskControl>>,
    semaphore: Option<Arc<Semaphore>>,
    observer: Option<Observer>,
    start_gate: Mutex<()>,
    started: AtomicBool,
    cancel_requested: AtomicBool,
    cancel_reason: Mutex<Option<crate::CancelReason>>,
    force: watch::Sender<bool>,
    done: watch::Sender<bool>,
    run: Mutex<RunState>,
}

struct ExecutionGuard<'a> {
    dag: &'a Scheduler,
    ctx: CancellationToken,
    handles: Vec<AbortHandle>,
    deadline: Option<AbortHandle>,
    finished: bool,
}

impl Drop for ExecutionGuard<'_> {
    fn drop(&mut self) {
        if let Some(deadline) = &self.deadline {
            deadline.abort();
        }
        if self.finished {
            return;
        }
        self.ctx.cancel();
        for handle in &self.handles {
            handle.abort();
        }
        let mut run = self.dag.run.lock().unwrap();
        let mut cancel_reported = run.cancel_reported;
        for status in run.statuses.values_mut() {
            if !status.state.done() {
                status.state = State::Canceled;
                status.err = cancellation_error(
                    Some(Error::ContextCancelled(
                        "execution future was dropped".into(),
                    )),
                    &mut cancel_reported,
                )
                .map(Arc::new);
            }
        }
        run.cancel_reported = cancel_reported;
        run.phase = if run
            .statuses
            .values()
            .any(|status| status.state == State::Failed)
        {
            Phase::Failed
        } else if run
            .statuses
            .values()
            .any(|status| status.state == State::Canceled)
        {
            Phase::Canceled
        } else {
            Phase::Success
        };
        drop(run);
        self.dag.done.send_replace(true);
    }
}

impl Scheduler {
    pub fn new(
        tasks: HashMap<String, BoxTask>,
        options: impl IntoIterator<Item = SchedulerOption>,
    ) -> Result<Self, Error> {
        let mut resolved = Options::default();
        for option in options {
            option.apply(&mut resolved);
        }
        let options = resolved;
        let mut names: Vec<_> = tasks.keys().cloned().collect();
        names.sort();

        let dependencies = snapshot_dependencies(&tasks)?;

        let mut dependents: HashMap<String, Vec<OutgoingEdge>> = HashMap::new();
        let mut in_degrees = HashMap::new();
        let mut statuses = HashMap::new();
        let mut controls = HashMap::new();
        for name in &names {
            let actual_name = tasks[name].name();
            if actual_name != name {
                return Err(Error::TaskNameMismatch {
                    key: name.clone(),
                    name: actual_name.into(),
                });
            }
            in_degrees.insert(name.clone(), dependencies[name].edges.len());
            statuses.insert(
                name.clone(),
                TaskResult {
                    state: State::Pending,
                    err: None,
                    attempts: 0,
                },
            );
            controls.insert(name.clone(), Arc::new(TaskControl::new(name.clone())));
            for edge in &dependencies[name].edges {
                dependents
                    .entry(edge.task.clone())
                    .or_default()
                    .push(OutgoingEdge {
                        to: name.clone(),
                        on: edge.on.clone(),
                    });
            }
        }

        for edges in dependents.values_mut() {
            edges.sort_unstable_by(|a, b| (&a.to, &a.on).cmp(&(&b.to, &b.on)));
        }

        let degrees = in_degrees.clone();
        let (force, _) = watch::channel(false);
        let (done, _) = watch::channel(false);

        Ok(Self {
            tasks,
            dependencies,
            dependents,
            in_degrees,
            controls,
            semaphore: (options.max_concurrency > 0).then(|| {
                Arc::new(Semaphore::new(
                    (options.max_concurrency as usize).min(Semaphore::MAX_PERMITS),
                ))
            }),
            observer: options.observer,
            start_gate: Mutex::new(()),
            started: AtomicBool::new(false),
            cancel_requested: AtomicBool::new(false),
            cancel_reason: Mutex::new(None),
            force,
            done,
            run: Mutex::new(RunState {
                phase: Phase::Pending,
                statuses,
                outputs: HashMap::new(),
                emitted: HashMap::new(),
                order: Vec::new(),
                cancel_reported: false,
                run_suspended: false,
                degrees,
            }),
        })
    }

    // The indegree reaches zero only after every incoming edge's source has settled.
    fn prepare(&self, name: &str) -> (State, Input) {
        let spec = &self.dependencies[name];
        let run = self.run.lock().unwrap();
        let mut active = Vec::new();
        for edge in &spec.edges {
            let state = run.statuses[&edge.task].state;
            if state == State::Canceled {
                // Cancellation propagates even if another edge would satisfy AnyOf.
                return (State::Canceled, Input::default());
            }
            if edge_is_active(state, run.emitted.get(&edge.task), &edge.on) {
                active.push(edge);
            }
        }
        let ready = spec.edges.is_empty()
            || match spec.join {
                JoinPolicy::AllOf => active.len() == spec.edges.len(),
                JoinPolicy::AnyOf => !active.is_empty(),
            };
        if !ready {
            return (State::Skipped, Input::default());
        }
        let mut input = Input::default();
        for edge in active {
            if edge.on == ON_FAILURE {
                input
                    .errors
                    .entry(edge.task.clone())
                    .or_insert_with(|| run.statuses[&edge.task].err.as_ref().unwrap().clone());
            } else {
                input
                    .outputs
                    .entry(edge.task.clone())
                    .or_insert_with(|| run.outputs[&edge.task].clone());
            }
        }
        (State::Pending, input)
    }

    fn spawn(
        name: String,
        task: BoxTask,
        blocked: State,
        input: Input,
        ctx: TaskContext,
        semaphore: Option<Arc<Semaphore>>,
    ) -> (impl Future<Output = (String, Completion)>, AbortHandle) {
        let task_name = name.clone();
        let join = tokio::spawn(async move {
            let _release = ctx.release_on_drop();
            Self::run_task(task_name.clone(), task, blocked, input, ctx, semaphore).await
        });
        let abort = join.abort_handle();
        (
            async move {
                let completion = match join.await {
                    Ok(completion) => completion,
                    Err(error) if error.is_panic() => {
                        Completion::failed(Self::task_panic(name.clone(), 0, error.into_panic()), 0)
                    }
                    Err(_) => Completion::failed(Error::TaskAbandoned(name.clone()), 0),
                };
                (name, completion)
            },
            abort,
        )
    }

    async fn run_task(
        name: String,
        task: BoxTask,
        blocked: State,
        input: Input,
        ctx: TaskContext,
        semaphore: Option<Arc<Semaphore>>,
    ) -> Completion {
        if ctx.is_cancelled() {
            return Completion {
                state: State::Canceled,
                output: None,
                error: Some(ctx.cancellation_error(None)),
                attempts: 0,
            };
        }
        if blocked != State::Pending {
            return Completion {
                state: blocked,
                output: None,
                error: None,
                attempts: 0,
            };
        }

        let policy = Retry::new(task.retry_policy());
        let control = ctx.control().clone();
        let (result, attempts) = policy
            .run(ctx.token(), semaphore, Some(control.clone()), |attempt| {
                control.attempts.store(attempt, Ordering::Release);
                let task = task.clone();
                let ctx = ctx.clone();
                let input = &input;
                let name = name.clone();
                async move {
                    let attempt_result = std::panic::AssertUnwindSafe(async {
                        task.pre_execution(ctx.clone(), attempt, input).await;
                        let (output, err) = task.execute(ctx.clone(), attempt, input).await;
                        task.post_execution(ctx, attempt, &output, err.as_ref())
                            .await;
                        if let Some(err) = err {
                            return Err(err);
                        }
                        if output.emit.iter().any(|trigger| trigger == ON_FAILURE) {
                            return Err(Error::InvalidEmit {
                                task: name.clone(),
                                trigger: ON_FAILURE.into(),
                            });
                        }
                        Ok(output)
                    })
                    .catch_unwind()
                    .await;
                    match attempt_result {
                        Ok(result) => result,
                        Err(payload) => Err(Self::task_panic(name, attempt, payload)),
                    }
                }
            })
            .await;

        match result {
            Ok(output) => Completion {
                state: State::Success,
                output: Some(output),
                error: None,
                attempts,
            },
            Err(error) => {
                let interrupted = ctx.is_cancelled() && error.is_context_error();
                let canceled = interrupted || error.is_run_canceled();
                Completion {
                    state: if canceled {
                        State::Canceled
                    } else {
                        State::Failed
                    },
                    output: None,
                    error: Some(if interrupted {
                        ctx.cancellation_error(Some(error))
                    } else {
                        error
                    }),
                    attempts,
                }
            }
        }
    }

    fn force_settle(&self) -> Vec<Event> {
        // Upstream abandonment reports the run-canceled sentinel, not the
        // caller's cause (which is still visible to the live task context).
        let reason = CancelReason::new(CancelKind::Run, None);
        let mut run = self.run.lock().unwrap();
        let mut names: Vec<_> = run.statuses.keys().cloned().collect();
        names.sort();
        let mut events = Vec::new();
        for name in names {
            let status = run.statuses.get_mut(&name).unwrap();
            if status.state.done() {
                continue;
            }
            status.state = State::Canceled;
            status.attempts = self.controls[&name].attempts.load(Ordering::Acquire);
            status.err = Some(Arc::new(Error::RunCanceled {
                reason: reason.clone(),
                last: None,
            }));
            events.push(Event {
                task: name,
                task_result: status.clone(),
            });
        }
        events
    }

    fn task_panic(task: String, attempt: i64, payload: Box<dyn Any + Send>) -> Error {
        let message = payload
            .downcast_ref::<Error>()
            .map(ToString::to_string)
            .unwrap_or_else(|| crate::observer::panic_message(payload.as_ref()));
        Error::TaskPanic {
            task,
            attempt,
            message,
            stack: std::backtrace::Backtrace::force_capture().to_string(),
            source: payload.downcast::<Error>().ok(),
        }
    }

    fn notify(&self, event: Event, jobs: &mut Vec<(String, JoinHandle<Result<(), Error>>)>) {
        if let Some(observer) = &self.observer {
            let observer = observer.clone();
            let task = event.task.clone();
            let job =
                tokio::task::spawn_blocking(move || crate::observer::notify(&*observer, event));
            jobs.push((task, job));
        }
    }

    pub async fn execute(&self, ctx: Context) -> (crate::Outputs, Option<RunError>) {
        let start_guard = self.start_gate.lock().unwrap();
        if self.started.swap(true, Ordering::AcqRel) {
            return (
                HashMap::new(),
                Some(RunError {
                    failures: Vec::new(),
                    observer_failures: Vec::new(),
                    run_error: Some(Box::new(Error::AlreadyExecuted)),
                }),
            );
        }
        self.run.lock().unwrap().phase = Phase::Running;
        let mut guard = ExecutionGuard {
            dag: self,
            ctx: ctx.token().child_token(),
            handles: Vec::new(),
            deadline: None,
            finished: false,
        };

        if ctx.error().is_some() {
            guard.ctx.cancel();
        } else if let Some(deadline) = ctx.deadline {
            let token = guard.ctx.clone();
            guard.deadline = Some(
                tokio::spawn(async move {
                    tokio::time::sleep_until(deadline).await;
                    token.cancel();
                })
                .abort_handle(),
            );
        }

        let contexts: HashMap<_, _> = self
            .controls
            .iter()
            .map(|(name, control)| (name.clone(), control.bind(&guard.ctx, &ctx)))
            .collect();

        let mut roots: Vec<_> = self
            .in_degrees
            .iter()
            .filter(|(_, count)| **count == 0)
            .map(|(name, _)| name.clone())
            .collect();
        roots.sort();
        let mut running = FuturesUnordered::new();
        let mut observer_jobs = Vec::new();
        let mut force = self.force.subscribe();
        let mut remaining = self.tasks.len();
        for name in roots {
            let (blocked, input) = self.prepare(&name);
            let (future, handle) = Self::spawn(
                name.clone(),
                self.tasks[&name].clone(),
                blocked,
                input,
                contexts[&name].clone(),
                self.semaphore.clone(),
            );
            guard.handles.push(handle);
            running.push(future);
        }
        drop(start_guard);

        while remaining > 0 {
            if *force.borrow_and_update() {
                for event in self.force_settle() {
                    self.notify(event, &mut observer_jobs);
                }
                break;
            }
            let next = tokio::select! {
                biased;
                changed = force.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    continue;
                }
                completion = running.next() => completion,
            };
            let Some((name, completed)) = next else {
                break;
            };
            let mut ready = Vec::new();
            let event = {
                let mut run = self.run.lock().unwrap();
                let error = if completed.state == State::Canceled {
                    if self.controls[&name]
                        .reason()
                        .is_some_and(|reason| reason.kind == CancelKind::Task)
                        || (contexts[&name]
                            .cause()
                            .is_some_and(|reason| reason.kind == CancelKind::TaskDone)
                            && completed.error.as_ref().is_some_and(Error::is_run_canceled))
                    {
                        completed.error
                    } else {
                        cancellation_error(completed.error, &mut run.cancel_reported)
                    }
                } else {
                    completed.error
                };
                let status = run.statuses.get_mut(&name).unwrap();
                if status.state.done() {
                    continue;
                }
                status.state = completed.state;
                status.attempts = completed.attempts;
                status.err = error.map(Arc::new);
                if let Some(output) = completed.output {
                    let (data, labels) = output.into_parts();
                    run.order.push(name.clone());
                    run.outputs.insert(name.clone(), data);
                    run.emitted.insert(name.clone(), labels);
                }
                if let Some(children) = self.dependents.get(&name) {
                    for edge in children {
                        let count = run.degrees.get_mut(&edge.to).unwrap();
                        *count -= 1;
                        if *count == 0 {
                            ready.push(edge.to.clone());
                        }
                    }
                }
                Event {
                    task: name.clone(),
                    task_result: run.statuses[&name].clone(),
                }
            };
            remaining -= 1;
            for child in ready {
                let (blocked, input) = self.prepare(&child);
                let (future, handle) = Self::spawn(
                    child.clone(),
                    self.tasks[&child].clone(),
                    blocked,
                    input,
                    contexts[&child].clone(),
                    self.semaphore.clone(),
                );
                guard.handles.push(handle);
                running.push(future);
            }
            self.notify(event, &mut observer_jobs);
        }

        let mut observer_failures = Vec::new();
        for (task, job) in observer_jobs {
            let error = match job.await {
                Ok(result) => result.err(),
                Err(join_error) => Some(Error::ObserverPanic {
                    task: task.clone(),
                    message: join_error.to_string(),
                    stack: std::backtrace::Backtrace::force_capture().to_string(),
                }),
            };
            if let Some(error) = error {
                observer_failures.push(ObserverFailure {
                    task,
                    error: Arc::new(error),
                });
            }
        }

        let mut run = self.run.lock().unwrap();
        let outputs = std::mem::take(&mut run.outputs);
        run.emitted.clear();
        let mut names: Vec<_> = run.statuses.keys().cloned().collect();
        names.sort();
        let mut failures = Vec::new();
        let mut failed = false;
        let mut canceled = false;
        for name in names {
            let status = &run.statuses[&name];
            if status.state == State::Failed {
                failed = true;
            }
            if status.state == State::Canceled {
                canceled = true;
            }
            if let Some(error) = &status.err {
                failures.push(TaskFailure {
                    task: name,
                    error: error.clone(),
                });
            }
        }
        run.phase = if failed {
            Phase::Failed
        } else if canceled {
            Phase::Canceled
        } else {
            Phase::Success
        };
        guard.finished = true;
        drop(run);
        self.done.send_replace(true);
        if !failures.is_empty() || !observer_failures.is_empty() {
            (
                outputs,
                Some(RunError {
                    failures,
                    observer_failures,
                    run_error: None,
                }),
            )
        } else {
            (outputs, None)
        }
    }
}

pub fn validate(tasks: &HashMap<String, BoxTask>) -> Result<(), Error> {
    snapshot_dependencies(tasks).map(|_| ())
}

fn snapshot_dependencies(
    tasks: &HashMap<String, BoxTask>,
) -> Result<HashMap<String, DependencySpec>, Error> {
    let mut names: Vec<_> = tasks.keys().cloned().collect();
    names.sort();
    let mut dependencies = HashMap::with_capacity(tasks.len());
    for name in &names {
        let task = &tasks[name];
        let mut spec = task.dependencies();
        for edge in &mut spec.edges {
            edge.on = normalize(std::mem::take(&mut edge.on));
        }
        dependencies.insert(name.clone(), spec);
    }
    for name in &names {
        for edge in &dependencies[name].edges {
            if !tasks.contains_key(&edge.task) {
                return Err(Error::UnknownDependency {
                    task: name.clone(),
                    dependency: edge.task.clone(),
                });
            }
        }
    }

    let mut colors = HashMap::new();
    let mut path = Vec::new();
    for name in names {
        if let Some(cycle) = visit(&name, &dependencies, &mut colors, &mut path) {
            return Err(Error::CircularDependency(cycle.join(" -> ")));
        }
    }
    Ok(dependencies)
}

fn visit(
    name: &str,
    dependencies: &HashMap<String, DependencySpec>,
    colors: &mut HashMap<String, u8>,
    path: &mut Vec<String>,
) -> Option<Vec<String>> {
    match colors.get(name) {
        Some(2) => return None,
        Some(1) => {
            let start = path.iter().position(|item| item == name).unwrap();
            let mut cycle = path[start..].to_vec();
            cycle.push(name.to_owned());
            return Some(cycle);
        }
        _ => {}
    }
    colors.insert(name.to_owned(), 1);
    path.push(name.to_owned());
    for edge in &dependencies[name].edges {
        if let Some(cycle) = visit(&edge.task, dependencies, colors, path) {
            return Some(cycle);
        }
    }
    path.pop();
    colors.insert(name.to_owned(), 2);
    None
}

#[cfg(test)]
mod tests {
    use super::cancellation_error;
    use crate::Error;

    #[test]
    fn canceled_tasks_keep_independent_last_errors_without_repeating_run_cause() {
        let mut reported = false;
        let first = cancellation_error(
            Some(Error::ContextCancelled("run stopped".into())),
            &mut reported,
        );
        assert!(matches!(first, Some(Error::ContextCancelled(_))));
        assert!(reported);

        let second = cancellation_error(
            Some(Error::RetryInterrupted {
                last: Box::new(Error::TaskExecution("network unavailable".into())),
            }),
            &mut reported,
        );
        assert!(
            matches!(second, Some(Error::TaskExecution(message)) if message == "network unavailable")
        );
        assert!(cancellation_error(
            Some(Error::ContextCancelled("same run".into())),
            &mut reported
        )
        .is_none());
    }
}
