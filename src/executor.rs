mod control;
mod graph;
mod query;

use crate::control::TaskControl;
use crate::options::Observer;
use crate::retry::Retry;
use crate::{
    BoxTask, CancelKind, CancelReason, DagOptions, Error, Event, ObserverFailure, RunError,
    RunPhase, TaskContext, TaskFailure, TaskInput, TaskResult, TaskState, TaskStatus,
};
use futures::future::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{watch, Semaphore};
use tokio::task::{AbortHandle, JoinHandle};
use tokio_util::sync::CancellationToken;

struct RunState {
    phase: RunPhase,
    statuses: HashMap<String, TaskStatus>,
    outputs: HashMap<String, TaskResult>,
    order: Vec<String>,
    cancel_reported: bool,
    run_suspended: bool,
    degrees: HashMap<String, usize>,
}

struct Completion {
    state: TaskState,
    output: Option<TaskResult>,
    error: Option<Error>,
    attempts: u64,
}

impl Completion {
    fn failed(error: Error, attempts: u64) -> Self {
        Self {
            state: TaskState::Failed,
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
        Some(Error::RetryInterrupted { last }) => Some(*last),
        _ => None,
    }
}

pub struct Dag {
    tasks: HashMap<String, BoxTask>,
    dependencies: HashMap<String, Vec<String>>,
    dependents: HashMap<String, Vec<String>>,
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
    dag: &'a Dag,
    ctx: CancellationToken,
    handles: Vec<AbortHandle>,
    finished: bool,
}

impl Drop for ExecutionGuard<'_> {
    fn drop(&mut self) {
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
                status.state = TaskState::Canceled;
                status.error = cancellation_error(
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
            .any(|status| status.state == TaskState::Failed)
        {
            RunPhase::Failed
        } else if run
            .statuses
            .values()
            .any(|status| status.state == TaskState::Canceled)
        {
            RunPhase::Canceled
        } else {
            RunPhase::Success
        };
        drop(run);
        self.dag.done.send_replace(true);
    }
}

impl Dag {
    pub fn new(tasks: HashMap<String, BoxTask>) -> Result<Self, Error> {
        Self::with_options(tasks, DagOptions::default())
    }

    pub fn with_options(
        tasks: HashMap<String, BoxTask>,
        options: DagOptions,
    ) -> Result<Self, Error> {
        let mut names: Vec<_> = tasks.keys().cloned().collect();
        names.sort();

        let dependencies = snapshot_dependencies(&tasks, true)?;

        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
        let mut in_degrees = HashMap::new();
        let mut statuses = HashMap::new();
        let mut controls = HashMap::new();
        for name in &names {
            in_degrees.insert(name.clone(), dependencies[name].len());
            statuses.insert(
                name.clone(),
                TaskStatus {
                    state: TaskState::Pending,
                    error: None,
                    attempts: 0,
                },
            );
            controls.insert(name.clone(), Arc::new(TaskControl::new(name.clone())));
            for dependency in &dependencies[name] {
                dependents
                    .entry(dependency.clone())
                    .or_default()
                    .push(name.clone());
            }
        }
        for children in dependents.values_mut() {
            children.sort();
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
                phase: RunPhase::Pending,
                statuses,
                outputs: HashMap::new(),
                order: Vec::new(),
                cancel_reported: false,
                run_suspended: false,
                degrees,
            }),
        })
    }

    // Every ready task receives a snapshot after all of its dependencies have settled.
    fn prepare(&self, name: &str) -> (TaskState, TaskInput) {
        let run = self.run.lock().unwrap();
        let mut blocked = TaskState::Pending;
        for dependency in &self.dependencies[name] {
            match run.statuses[dependency].state {
                TaskState::Canceled => blocked = TaskState::Canceled,
                TaskState::Success => {}
                _ if blocked != TaskState::Canceled => blocked = TaskState::Skipped,
                _ => {}
            }
        }
        if blocked != TaskState::Pending {
            return (blocked, HashMap::new());
        }
        let input = self.dependencies[name]
            .iter()
            .map(|dependency| {
                let values = run.outputs[dependency]
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                (dependency.clone(), serde_json::Value::Object(values))
            })
            .collect();
        (TaskState::Pending, input)
    }

    fn spawn(
        name: String,
        task: BoxTask,
        blocked: TaskState,
        input: TaskInput,
        ctx: TaskContext,
        semaphore: Option<Arc<Semaphore>>,
    ) -> (impl Future<Output = (String, Completion)>, AbortHandle) {
        let task_name = name.clone();
        let join = tokio::spawn(async move {
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
                    Err(error) => Completion::failed(
                        Error::TaskExecution(format!("task {name:?} abandoned: {error}")),
                        0,
                    ),
                };
                (name, completion)
            },
            abort,
        )
    }

    async fn run_task(
        name: String,
        task: BoxTask,
        blocked: TaskState,
        input: TaskInput,
        ctx: TaskContext,
        semaphore: Option<Arc<Semaphore>>,
    ) -> Completion {
        if ctx.is_cancelled() {
            return Completion {
                state: TaskState::Canceled,
                output: None,
                error: Some(ctx.cancellation_error(None)),
                attempts: 0,
            };
        }
        if blocked != TaskState::Pending {
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
                let input = input.clone();
                let name = name.clone();
                async move {
                    let attempt_result = std::panic::AssertUnwindSafe(async {
                        task.pre_execution(ctx.clone(), attempt, &input).await?;
                        let outcome = task.execute(ctx.clone(), attempt, &input).await;
                        let post = task
                            .post_execution(
                                ctx,
                                attempt,
                                outcome.output.as_ref(),
                                outcome.error.as_ref(),
                            )
                            .await;
                        match (outcome.output, outcome.error, post) {
                            (_, Some(original), Err(post)) => Err(Error::TaskAndPostExecution {
                                execute: Box::new(original),
                                post: Box::new(post),
                            }),
                            (_, _, Err(post)) => Err(post),
                            (_, Some(error), Ok(())) => Err(error),
                            (Some(output), None, Ok(())) => Ok(output),
                            (None, None, Ok(())) => Ok(TaskResult::new()),
                        }
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
                state: TaskState::Success,
                output: Some(output),
                error: None,
                attempts,
            },
            Err(error) => {
                let interrupted = ctx.is_cancelled()
                    && matches!(
                        &error,
                        Error::ContextCancelled(_) | Error::RetryInterrupted { .. }
                    );
                let canceled = interrupted || error.is_run_canceled();
                Completion {
                    state: if canceled {
                        TaskState::Canceled
                    } else {
                        TaskState::Failed
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
        let reason = self
            .cancel_reason
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| CancelReason::new(CancelKind::Run, None));
        let mut run = self.run.lock().unwrap();
        let mut names: Vec<_> = run.statuses.keys().cloned().collect();
        names.sort();
        let mut events = Vec::new();
        for name in names {
            let status = run.statuses.get_mut(&name).unwrap();
            if status.state.done() {
                continue;
            }
            status.state = TaskState::Canceled;
            status.attempts = self.controls[&name].attempts.load(Ordering::Acquire);
            status.error = Some(Arc::new(Error::RunCanceled {
                reason: reason.clone(),
                last: None,
            }));
            events.push(Event {
                task: name,
                status: status.clone(),
            });
        }
        events
    }

    fn task_panic(task: String, attempt: u64, payload: Box<dyn Any + Send>) -> Error {
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

    pub async fn execute(
        &self,
        ctx: CancellationToken,
    ) -> Result<HashMap<String, TaskResult>, RunError> {
        let start_guard = self.start_gate.lock().unwrap();
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(RunError {
                outputs: HashMap::new(),
                failures: Vec::new(),
                observer_failures: Vec::new(),
                run_error: Some(Box::new(Error::AlreadyExecuted)),
            });
        }
        self.run.lock().unwrap().phase = RunPhase::Running;
        let mut guard = ExecutionGuard {
            dag: self,
            ctx: ctx.child_token(),
            handles: Vec::new(),
            finished: false,
        };

        let contexts: HashMap<_, _> = self
            .controls
            .iter()
            .map(|(name, control)| (name.clone(), control.bind(&guard.ctx)))
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
                let error = if completed.state == TaskState::Canceled {
                    if self.controls[&name]
                        .reason()
                        .is_some_and(|reason| reason.kind == CancelKind::Task)
                        || (!contexts[&name].is_cancelled()
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
                status.error = error.map(Arc::new);
                if let Some(output) = completed.output {
                    run.order.push(name.clone());
                    run.outputs.insert(name.clone(), output);
                }
                if let Some(children) = self.dependents.get(&name) {
                    for child in children {
                        let count = run.degrees.get_mut(child).unwrap();
                        *count -= 1;
                        if *count == 0 {
                            ready.push(child.clone());
                        }
                    }
                }
                Event {
                    task: name.clone(),
                    status: run.statuses[&name].clone(),
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
        let outputs = run.outputs.clone();
        let mut names: Vec<_> = run.statuses.keys().cloned().collect();
        names.sort();
        let mut failures = Vec::new();
        let mut failed = false;
        let mut canceled = false;
        for name in names {
            let status = &run.statuses[&name];
            if status.state == TaskState::Failed {
                failed = true;
            }
            if status.state == TaskState::Canceled {
                canceled = true;
            }
            if let Some(error) = &status.error {
                failures.push(TaskFailure {
                    task: name,
                    error: error.clone(),
                });
            }
        }
        run.phase = if failed {
            RunPhase::Failed
        } else if canceled {
            RunPhase::Canceled
        } else {
            RunPhase::Success
        };
        guard.finished = true;
        drop(run);
        self.done.send_replace(true);
        if failed || canceled || !observer_failures.is_empty() {
            Err(RunError {
                outputs,
                failures,
                observer_failures,
                run_error: None,
            })
        } else {
            Ok(outputs)
        }
    }
}

pub fn validate(tasks: &HashMap<String, BoxTask>) -> Result<(), Error> {
    snapshot_dependencies(tasks, false).map(|_| ())
}

fn snapshot_dependencies(
    tasks: &HashMap<String, BoxTask>,
    check_names: bool,
) -> Result<HashMap<String, Vec<String>>, Error> {
    let mut names: Vec<_> = tasks.keys().cloned().collect();
    names.sort();
    let mut dependencies = HashMap::with_capacity(tasks.len());
    for name in &names {
        let task = &tasks[name];
        if check_names {
            let actual_name = task.name();
            if actual_name != name {
                return Err(Error::TaskNameMismatch {
                    key: name.clone(),
                    name: actual_name.into(),
                });
            }
        }
        dependencies.insert(name.clone(), task.dependencies());
    }
    for name in &names {
        for dependency in &dependencies[name] {
            if !tasks.contains_key(dependency) {
                return Err(Error::UnknownDependency {
                    task: name.clone(),
                    dependency: dependency.clone(),
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
    dependencies: &HashMap<String, Vec<String>>,
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
    for dependency in &dependencies[name] {
        if let Some(cycle) = visit(dependency, dependencies, colors, path) {
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
