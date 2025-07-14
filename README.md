# Dagcuter 🚀

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE) [![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org) [![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]

[RustDagcuter](https://crates.io/crates/dagcuter) is a Rust library for executing directed acyclic graphs (DAGs) of tasks. It manages task dependencies, detects cyclic dependencies, and supports customizable task lifecycles (pre-execution, post-execution). It also supports concurrent execution of independent tasks to improve performance.

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
│ ├─ task.rs # Task features and hooks
│ ├─ retry.rs # Retry strategy
│ ├─ cycle_check.rs # Cycle detection algorithm
│ └─ executor.rs # Executor core
├─ examples/ # Example code
├─ Cargo.toml
└─ README.md
````

## 🚀 Quick start

1. Add dependencies in `Cargo.toml`:

```toml
[dependencies]
dagcuter = "0.1.1"
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
use dagcuter::*; 
use async_trait::async_trait; 
use tokio_util::sync::CancellationToken; 
use std::{collections::HashMap, sync::Arc}; 

struct ExampleTask { 
name: String, 
deps: Vec<String>, 
} 

#[async_trait] 
impl Task for ExampleTask { 
fn name(&self) -> &str { &self.name } 
fn dependencies(&self) -> Vec<String> { self.deps.clone() } 
fn retry_policy(&self) -> Option<RetryPolicy> { 
Some(RetryPolicy { max_attempts: 3, ..Default::default() }) 
} 
async fn execute( 
&self, 
_ctx: CancellationToken, 
_input: &TaskInput, 
) -> Result<TaskResult, DagcuterError> { 
println!("Execute task: {}", self.name); 
tokio::time::sleep(std::time::Duration::from_millis(100)).await; 
let mut out = HashMap::new(); 
out.insert("status".into(), serde_json::json!("ok")); 
Ok(out) 
} 
} 

#[tokio::main] 
async fn main() { 
let mut tasks: HashMap<String, BoxTask> = HashMap::new(); 
tasks.insert("A".into(), Arc::new(ExampleTask { name: "A".into(), deps: vec![] })); 
tasks.insert("B".into(), Arc::new(ExampleTask { name: "B".into(), deps: vec!["A".into()] })); 

let mut engine = Dagcuter::new(tasks).unwrap(); 
let ctx = CancellationToken::new(); 

println!("=== dependency graph ==="); 
engine.print_graph(); 

println!("=== Start execution ==="); 
let results = engine.execute(ctx.clone()).await.unwrap(); 
println!("=== completion: {:?} ===", results); 
} ```

3. Run the example: 

```bash 
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
fn retry_policy(&self) -> Option<RetryPolicy> { None } 
async fn pre_execution(&self, _ctx: CancellationToken, _input: &TaskInput) -> Result<(), DagcuterError> { Ok(()) } 
async fn execute(&self, ctx: CancellationToken, input: &TaskInput) -> Result<TaskResult, DagcuterError>; 
async fn post_execution(&self, _ctx: CancellationToken, _output: &TaskResult) -> Result<(), DagcuterError> { Ok(()) }
}
```

### `RetryPolicy`

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetryPolicy { 
pub interval: Duration, 
pub max_interval: Duration, 
pub max_attempts: i32, 
pub multiplier: f64,
}

impl Default for RetryPolicy { 
fn default() -> Self { /* 1s, 30s, -1, 2.0 */ }
}
```

### `Dagcuter`

```rust
impl Dagcuter { 
pub fn new(tasks: HashMap<String, BoxTask>) -> Result<Self, DagcuterError>;
pub async fn execute(&mut self, ctx: CancellationToken) -> Result<HashMap<String, TaskResult>, DagcuterError>;
pub async fn execution_order(&self) -> Vec<String>;
pub fn print_graph(&self);
}
```

## 🔧 Advanced usage

* Custom retry: adjust `interval`, `multiplier`, `max_attempts`

* Lifecycle hook: override `pre_execution`/`post_execution`

* Cancellation and timeout: combine `CancellationToken` to control execution

* Complex data flow: process `TaskInput` in `execute` and return a custom `TaskResult`

## 📝 License

This project adopts the MIT protocol, see [LICENSE](LICENSE) for details.