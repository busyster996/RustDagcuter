# Dagcuter 🚀

[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE) [![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)

[RustDagcuter](https://crates.io/crates/rs-dagcuter) is a Rust library for executing directed acyclic graphs (DAGs) of tasks. It manages task dependencies, detects cyclic dependencies, and supports customizable task lifecycles (pre-execution, post-execution). It also supports concurrent execution of independent tasks to improve performance.

Alignment with [xdag](https://github.com/xmapst/xdag) includes graph validation, failure/cancellation propagation, retry with jitter, deterministic graph output, concurrency limits, progress, observers, task controls, suspension and whole-run cancellation with a grace period. The Rust API uses `TaskContext` for cancellation causes, while `TaskInput` and `TaskResult` remain JSON maps by design.

---

## ✨ Core functions

- **Intelligent dependency management**: Automatically parse and schedule multi-task dependencies.
- **Graph validation**: Reject missing dependencies, task-name mismatches and cycles during construction.
- **Failure propagation**: A failed task skips its dependents; cancellation propagates as cancellation while unrelated branches continue.
- **High concurrent execution**: Topological sorting drives parallel operation, making full use of multi-cores.
- **Exponential backoff retry**: Configurable attempts, interval caps, downward jitter and non-retryable errors.
- **Cooperative cancellation**: Tasks receive a cancelable context with an inspectable cause; whole-run cancel can force settlement after a grace period.
- **Execution tracking**: Query task states and completion order during execution.
- **Concurrency control**: Limit active attempts without occupying slots during retry backoff.
- **Progress and observation**: Poll state counts or receive an event for every terminal task.
- **Task controls**: Cancel or suspend one task, or suspend and resume the whole run.
- **Type safety**: Static type guarantee, compile-time error checking.
- **Life cycle hook**: Custom logic can be inserted before/after task execution.

## 🏗️ Project structure

```text
dagcuter/
├─ src/
│ ├─ lib.rs # Core exports and type definitions
│ ├─ control.rs # Per-task contexts and suspension gates
│ ├─ executor.rs # DAG scheduling and states
│ ├─ executor/ # Graph output, queries and control commands
│ ├─ observer.rs # Terminal event types and panic handling
│ ├─ options.rs # Constructor options
│ ├─ progress.rs # Progress snapshots
│ └─ retry.rs # Retry and backoff
├─ example/ # Example code
│ ├─ src/
│ │ ├─ main.rs # Default successful DAG
│ │ └─ bin/ # Independent behavior scenarios
│ └─ Cargo.toml
├─ Cargo.toml
└─ README.md
```

## 🚀 Quick start

1. Add dependencies in `Cargo.toml`:

```toml
rs-dagcuter = { version = "0.2.0" }
tokio = { version = "1.0", features = ["full"] }
async-trait = "0.1"
tokio-util = "0.7"
serde_json = "1.0"
chrono = "0.4"
```

2. Write the task and execute it:

```rust 
use rs_dagcuter::*;
use async_trait::async_trait;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;
use std::sync::Arc;

// 示例任务实现
struct ExampleTask {
    name: String,
    deps: Vec<String>,
}

#[async_trait]
impl Task for ExampleTask {
    fn name(&self) -> &str {
        &self.name
    }

    fn dependencies(&self) -> Vec<String> {
        self.deps.clone()
    }

    fn retry_policy(&self) -> Option<RetryPolicy> {
        Some(RetryPolicy {
            max_attempts: 3,
            ..Default::default()
        })
    }

    async fn execute(
        &self,
        _ctx: TaskContext,
        _attempt: u64,
        _input: &TaskInput,
    ) -> TaskOutcome {
        println!("执行任务: {}", self.name);

        // 模拟任务执行时间
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut result = HashMap::new();
        result.insert("status".to_string(), serde_json::json!("completed"));
        result.insert("task_name".to_string(), serde_json::json!(self.name));
        result.insert("timestamp".to_string(), serde_json::json!(chrono::Utc::now().to_rfc3339()));
        TaskOutcome::success(result)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tasks: HashMap<String, BoxTask> = HashMap::new();

    tasks.insert("task1".to_string(), Arc::new(ExampleTask {
        name: "task1".to_string(),
        deps: vec![],
    }));

    tasks.insert("task2".to_string(), Arc::new(ExampleTask {
        name: "task2".to_string(),
        deps: vec!["task1".to_string()],
    }));

    tasks.insert("task3".to_string(), Arc::new(ExampleTask {
        name: "task3".to_string(),
        deps: vec!["task1".to_string()],
    }));

    tasks.insert("task4".to_string(), Arc::new(ExampleTask {
        name: "task4".to_string(),
        deps: vec!["task2".to_string(), "task3".to_string()],
    }));

    tasks.insert("task5".to_string(), Arc::new(ExampleTask {
        name: "task5".to_string(),
        deps: vec!["task2".to_string()],
    }));

    tasks.insert("task6".to_string(), Arc::new(ExampleTask {
        name: "task6".to_string(),
        deps: vec!["task1".to_string(), "task4".to_string(), "task5".to_string()],
    }));



    let dag = Dag::new(tasks)?;
    let ctx = CancellationToken::new();

    println!("=== 任务依赖图 ===");
    dag.print_graph();

    println!("=== 开始执行任务 ===");
    let start = std::time::Instant::now();
    let results = dag.execute(ctx).await?;
    let duration = start.elapsed();

    println!("=== 执行完成 ===");
    println!("执行时间: {:?}", duration);
    println!("执行结果: {:#?}", results);
    println!("执行顺序: {}", dag.execution_order().await);

    Ok(())
}
```

3. Run the example:

```bash 
cd example
cargo run 
```

---

## Runnable scenarios

`cd example && cargo run` still runs the successful DAG. The other binaries check their own expected outcomes and exit successfully even when the demonstrated tasks fail or are canceled:

| Command (from `example/`) | Behavior |
| --- | --- |
| `cargo run --bin validation` | Invalid graphs, failed dependency, skipped child, partial success |
| `cargo run --bin retry_hooks` | Retry, lifecycle hooks, output alongside error, non-retryable panic |
| `cargo run --bin task_control` | Independent suspension sources and targeted cancellation cause |
| `cargo run --bin observe` | Concurrency limit, progress, events, observer panic |
| `cargo run --bin cancel_grace` | Cooperative stop, grace expiry, discarded late result |

The task/observer panic demonstrations intentionally emit Rust's default panic-hook message; the scheduler catches those panics and each binary verifies the resulting state.

---

## 📚 API Overview

`validate(&tasks)` checks missing dependencies and cycles without constructing a `Dag`. `Dag::new` additionally rejects task names that do not match their map keys.

### `Task` trait

```rust
#[async_trait]
pub trait Task: Send + Sync {
    fn name(&self) -> &str;
    fn dependencies(&self) -> Vec<String>;
    fn retry_policy(&self) -> Option<RetryPolicy>;

    async fn pre_execution(
        &self,
        _ctx: TaskContext,
        _attempt: u64,
        _input: &TaskInput,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn execute(
        &self,
        ctx: TaskContext,
        attempt: u64,
        input: &TaskInput,
    ) -> TaskOutcome;

    async fn post_execution(
        &self,
        _ctx: TaskContext,
        _attempt: u64,
        _output: Option<&TaskResult>,
        _error: Option<&Error>,
    ) -> Result<(), Error> {
        Ok(())
    }
}
```

`TaskOutcome::success(output)` and `TaskOutcome::failure(error)` cover normal cases. `TaskOutcome::with_error(output, error)` lets `post_execution` see both values from one attempt; when `error` is present, `output` is not passed downstream or included in successful results. `TaskInput` and `TaskResult` retain their JSON map types.

### `RetryPolicy`

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub interval: Duration,         // Initial retry interval
    pub max_interval: Duration,     // Maximum retry interval
    pub max_attempts: i64,          // Total attempts; 0 = 1, negative = unlimited
    pub multiplier: f64,            // Retry interval exponential
    pub jitter: f64,                // Downward jitter in [0, 1]
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(30),
            max_attempts: 1,
            multiplier: 2.0,
            jitter: 0.0,
        }
    }
}

pub const INFINITE_ATTEMPTS: i64 = -1;
```

### `Dag`

```rust
impl Dag {
    /// Create a new DAG instance
    pub fn new(tasks: HashMap<String, BoxTask>) -> Result<Self, Error>;

    /// Configure an active-attempt limit and an optional terminal-state observer
    pub fn with_options(tasks: HashMap<String, BoxTask>, options: DagOptions) -> Result<Self, Error>;

    /// Execute all tasks in the DAG
    pub async fn execute(
        &self,
        ctx: CancellationToken,
    ) -> Result<HashMap<String, TaskResult>, RunError>;

    /// Snapshot of each task's terminal state, error and attempt count
    pub fn task_results(&self) -> HashMap<String, TaskStatus>;

    pub fn state(&self, name: &str) -> Option<TaskState>;
    pub fn states(&self) -> HashMap<String, TaskState>;
    pub fn phase(&self) -> RunPhase;
    pub fn progress(&self) -> Progress;

    pub fn cancel_task(&self, name: &str) -> Result<(), Error>;
    pub fn cancel_task_with_cause(&self, name: &str, cause: CancelCause) -> Result<(), Error>;
    pub fn suspend_task(&self, name: &str) -> Result<(), Error>;
    pub fn resume_task(&self, name: &str) -> Result<(), Error>;
    pub fn suspended_task(&self, name: &str) -> bool;
    pub fn suspend(&self);
    pub fn resume(&self);
    pub fn suspended(&self) -> bool;
    pub fn canceled(&self) -> bool;
    pub async fn cancel(&self, grace: Duration) -> Result<(), Error>;
    pub async fn cancel_with_cause(&self, grace: Duration, cause: CancelCause) -> Result<(), Error>;

    /// Get the execution order of the DAG
    pub async fn execution_order(&self) -> String;

    /// Print the DAG graph
    pub fn print_graph(&self);

    /// Write a stable graph view to any writer
    pub fn write_graph(&self, writer: &mut impl std::io::Write) -> std::io::Result<()>;
}
```

### `Error`

```rust
#[derive(Error, Debug)]
pub enum Error {
    #[error("circular dependency detected: {0}")]
    CircularDependency(String),

    #[error("task {task:?} depends on unknown task {dependency:?}")]
    UnknownDependency { task: String, dependency: String },

    #[error("task map key {key:?} does not match task name {name:?}")]
    TaskNameMismatch { key: String, name: String },

    #[error("this DAG has already been executed")]
    AlreadyExecuted,
    
    #[error("task execution failed: {0}")]
    TaskExecution(String),
    
    #[error("execution canceled: {0}")]
    ContextCancelled(String),
    
    #[error("retries exhausted: {last}")]
    RetryFailed { last: Box<Error> },

    #[error("non-retryable error: {0}")]
    NonRetryable(Box<Error>),

    #[error("execute: {execute}; post_execution: {post}")]
    TaskAndPostExecution { execute: Box<Error>, post: Box<Error> },

    #[error("execution canceled during retry wait; last attempt: {last}")]
    RetryInterrupted { last: Box<Error> },

    #[error("task {task:?} panicked on attempt {attempt}: {message}")]
    TaskPanic { task: String, attempt: u64, message: String, stack: String, source: Option<Box<Error>> },

    #[error("observer panicked on event for task {task:?}: {message}")]
    ObserverPanic { task: String, message: String, stack: String },

    #[error("unknown task {0:?}")]
    UnknownTask(String),

    #[error("task {0:?} already reached a terminal state")]
    TaskAlreadyDone(String),

    #[error("task {task:?} canceled")]
    TaskCanceled { task: String, reason: CancelReason, last: Option<Box<Error>> },

    #[error("run canceled")]
    RunCanceled { reason: CancelReason, last: Option<Box<Error>> },

    #[error("cancel grace period expired")]
    GracePeriodExpired,
}
```

## 🔧 Advanced usage

* Custom retry: adjust `interval`, `multiplier`, `max_attempts` and `jitter`; use `INFINITE_ATTEMPTS` for unlimited attempts.

* Permanent failures: return `Error::NonRetryable(Box::new(cause))` to stop retrying immediately.

* Lifecycle hook: override `pre_execution`/`post_execution`

* Cancellation and timeout: pass a `CancellationToken` to `execute`; task implementations receive a `TaskContext` and can await `ctx.cancelled()` or inspect `ctx.cause()` (`Task`, `Run` or `Parent`). Use `cancel_task(name)` to stop one task or `cancel(grace)` to stop the run and wait for a grace period. After grace expires, unresolved tasks are marked canceled and late results are discarded, but uncooperative task bodies may still be running.

* Suspension: `suspend_task`/`resume_task` and `suspend`/`resume` are independent; both must be resumed before the next attempt starts. Cancellation wakes suspended tasks. Tasks already executing are not paused mid-attempt.

* Partial results: on failure, `RunError.outputs` still holds outputs from successful tasks; `task_results()` exposes all states.

* Configure concurrency and observation with `DagOptions::default().max_concurrency(4).observer(|event| { /* record event */ })`. The observer runs after the task state is committed, may be called concurrently, and must not wait for `execute` to return. Observer panics appear in `RunError.observer_failures` without changing task states.

* Poll `dag.progress()` for `total`, `pending`, `success`, `skipped`, `canceled` and `failed` counts; `ratio()` is 1.0 for an empty graph.

* Each `Dag` runs only once; construct a fresh instance for another run.

* Complex data flow: process `TaskInput` in `execute` and return a custom `TaskResult`

## 📝 License

This project is licensed under Apache-2.0, see [LICENSE](LICENSE) for details.
