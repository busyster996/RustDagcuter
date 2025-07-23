# Dagcuter 🚀

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE) [![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)

[RustDagcuter](https://crates.io/crates/rs-dagcuter) is a Rust library for executing directed acyclic graphs (DAGs) of tasks. It manages task dependencies, detects cyclic dependencies, and supports customizable task lifecycles (pre-execution, post-execution). It also supports concurrent execution of independent tasks to improve performance.

---

## ✨ Core functions

- **Intelligent dependency management**: Automatically parse and schedule multi-task dependencies.
- **Loop detection**: Real-time discovery and prevention of loop dependencies.
- **High concurrent execution**: Topological sorting drives parallel operation, making full use of multi-cores.
- **Exponential backoff retry**: Built-in configurable retry strategy; supports custom intervals, multiples and maximum times.
- **Graceful cancellation**: Supports mid-way cancellation and resource release.
- **Execution tracking**: Real-time printing of task status and execution order.
- **Type safety**: Static type guarantee, compile-time error checking.
- **Zero cost abstraction**: Minimal runtime overhead.
- **Life cycle hook**: Custom logic can be inserted before/after task execution.

## 🏗️ Project structure

```text
dagcuter/
├─ src/
│ ├─ lib.rs # Core exports and type definitions
│ └─ executor.rs # DAG Executor Core Logic
├─ examples/ # Example code
| ├─ src/
| │ └─ main.rs
| └─ Cargo.toml
├─ Cargo.toml
└─ README.md
````

## 🚀 Quick start

1. Add dependencies in `Cargo.toml`:

```toml
[dependencies]
rs-dagcuter = "0.0.1"
tokio = { version = "1.0", features = ["full"] }
async-trait = "0.1"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
thiserror = "1.0"
futures = "0.3"
tokio-util = "0.7" 
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
        _ctx: CancellationToken,
        _input: &TaskInput,
    ) -> Result<TaskResult, Error> {
        println!("执行任务: {}", self.name);

        // 模拟任务执行时间
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut result = HashMap::new();
        result.insert("status".to_string(), serde_json::json!("completed"));
        result.insert("task_name".to_string(), serde_json::json!(self.name));
        result.insert("timestamp".to_string(), serde_json::json!(chrono::Utc::now().to_rfc3339()));
        Ok(result)
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



    let mut dag = Dag::new(tasks)?;
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

## 📚 API Overview

### `Task` attribute

```rust
#[async_trait]
pub trait Task: Send + Sync {
    fn name(&self) -> &str;
    fn dependencies(&self) -> Vec<String>;
    fn retry_policy(&self) -> Option<RetryPolicy>;

    async fn pre_execution(
        &self,
        _ctx: CancellationToken,
        _input: &TaskInput,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn execute(
        &self,
        ctx: CancellationToken,
        input: &TaskInput,
    ) -> Result<TaskResult, Error>;

    async fn post_execution(
        &self,
        _ctx: CancellationToken,
        _output: &TaskResult,
    ) -> Result<(), Error> {
        Ok(())
    }
}
```

### `RetryPolicy`

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub interval: Duration,         // Initial retry interval
    pub max_interval: Duration,     // Maximum retry interval
    pub max_attempts: i32,          // Maximum number of retries
    pub multiplier: f64,            // Retry interval exponential
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(30),
            max_attempts: 1,
            multiplier: 2.0,
        }
    }
}
```

### `Dag`

```rust
impl Dag {
    /// Create a new DAG instance
    pub fn new(tasks: HashMap<String, BoxTask>) -> Result<Self, Error>;

    /// Execute all tasks in the DAG
    pub async fn execute(
        &mut self,
        ctx: CancellationToken,
    ) -> Result<HashMap<String, TaskResult>, Error>;

    /// Get the execution order of the DAG
    pub async fn execution_order(&self) -> String;

    /// Print the DAG graph
    pub fn print_graph(&self);
}
```

### `Error`

```rust
#[derive(Error, Debug)]
pub enum Error {
    #[error("Circular dependency detected")]
    CircularDependency,
    
    #[error("Task execution failed: {0}")]
    TaskExecution(String),
    
    #[error("Context cancelled: {0}")]
    ContextCancelled(String),
    
    #[error("Retry failed: {0}")]
    RetryFailed(String),
}
```

## 🔧 Advanced usage

* Custom retry: adjust `interval`, `multiplier`, `max_attempts`

* Lifecycle hook: override `pre_execution`/`post_execution`

* Cancellation and timeout: combine `CancellationToken` to control execution

* Complex data flow: process `TaskInput` in `execute` and return a custom `TaskResult`

## 📝 License

This project adopts the MIT protocol, see [LICENSE](LICENSE) for details.
