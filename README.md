# RustDagcuter

A Tokio DAG scheduler targeting [xdag `9533372`](https://github.com/xmapst/xdag/tree/95333724258112c354f3ea618d72c58ba51bb42a). The API uses Rust traits, async calls and typed errors. See the [upstream test comparison](UPSTREAM_PARITY.md) for verified behaviors and remaining gaps.

## Quick start

Use this checkout as a path dependency and add Tokio and `async-trait`.

```rust
use async_trait::async_trait;
use rs_dagcuter::*;
use std::sync::Arc;

struct Step(&'static str);

#[async_trait]
impl Task for Step {
    fn name(&self) -> &str { self.0 }

    fn dependencies(&self) -> DependencySpec {
        match self.0 {
            "save" => after(["fetch"]),
            _ => DependencySpec::default(),
        }
    }

    async fn execute(&self, _: TaskContext, _: i64, input: &Input) -> (Output, Option<Error>) {
        if let Some(fetch) = input.outputs.get("fetch") {
            assert_eq!(fetch["count"].downcast_ref::<i32>(), Some(&42));
        }
        (Data::from([("count".into(), Arc::new(42_i32) as Value)]).into(), None)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tasks = ["fetch", "save"].into_iter()
        .map(|name| (name.into(), Arc::new(Step(name)) as BoxTask))
        .collect();
    let scheduler = Scheduler::new(tasks, [])?;
    let (outputs, err) = scheduler.execute(Context::new()).await;
    if let Some(err) = err { return Err(err.into()); }
    assert_eq!(outputs.len(), 2);
    Ok(())
}
```

## Labeled dependencies

- `after(["a", "b"])` creates `ON_SUCCESS` edges with an `AllOf` join. A successful task can choose custom labels in `Output.emit`; a nonempty list replaces the default. Empty labels mean `ON_SUCCESS`. Custom labels are matched literally, including case and whitespace.
- `ON_FAILURE` activates only when the upstream fails. The downstream receives the error through `Input.errors`. A successful handler does not erase the original failure. Emitting `ON_FAILURE` explicitly on success produces a non-retryable `Error::InvalidEmit`.
- Both joins wait until **all** direct upstream tasks settle. `AllOf` requires every edge to activate; `AnyOf` needs at least one. An unmatched join is `Skipped`, which does not by itself fail the run. An upstream cancellation takes precedence over either join and does not activate `ON_FAILURE`. Root tasks are not gated by a join.
- `Input.outputs` and `Input.errors` contain only active edges. Each downstream has independent maps with shared, opaque payloads (`Arc<dyn Any + Send + Sync>`), not JSON-only values. Treat payloads as read-only and use `downcast_ref` to recover their types.

This join runs cleanup whether `work` succeeds or fails:

```rust
use rs_dagcuter::{DependencyEdge, DependencySpec, JoinPolicy, ON_FAILURE, ON_SUCCESS};
let cleanup = DependencySpec {
    edges: vec![DependencyEdge::new("work", ON_SUCCESS), DependencyEdge::new("work", ON_FAILURE)],
    join: JoinPolicy::AnyOf,
};
```

## Execution, control and queries

- `Scheduler::new(tasks, options)` freezes the graph and rejects missing dependencies, cycles and task-name/key mismatches. `validate(&tasks)` checks graph topology without constructing a scheduler.
- `execute(context)` runs once and returns `(successful_outputs, Option<RunError>)`, including partial successes on failure. `results()` gives each task's state, error and attempt count; `state()` returns `Pending` for an unknown name. Overall phase precedence is `Failed`, then `Canceled`, then `Success`.
- `with_max_concurrency(n)` limits simultaneous attempts, not tasks waiting on retries or suspension. `with_observer(callback)` reports committed terminal results. Callbacks may run concurrently; a panic is reported separately in `RunError` without changing task state.
- `RetryPolicy::default()` is the zero policy and attempts once. Positive `max_attempts` is the total count; negative means unlimited. Backoff supports a multiplier, a cap and downward jitter. `pre_execution` and `post_execution` are notification hooks; a panic in either counts as an attempt failure. `post_execution` receives both output and error before the scheduler validates emitted labels.
- `Context::with_timeout` or `with_deadline` bounds execution. `cancel_task(name, options)` signals a single task; cancellation propagates to its descendants once it settles. `cancel(grace_context, options)` signals the run and waits. If grace ends first, unfinished tasks are settled as canceled and late results are discarded. In-flight task bodies can continue until they cooperate or return. Use `with_cause(...)` to attach a cause.
- `suspend` / `resume` and `suspend_task` / `resume_task` are independent gates. Neither interrupts an in-flight attempt. `Control` exposes just the control surface; `SchedulerApi` also exposes execution and queries.
- `progress()` returns state counts. `write_graph()` prints deterministic labels and `(any-of)` annotations. `success_completion_order()` lists only successful tasks in completion order, not start or topological order.

## Migrating from the previous API

No compatibility aliases are provided. Replace `Dag` with `Scheduler`, use constructor options such as `[with_max_concurrency(4)]`, and migrate `TaskState` / `RunPhase` to `State` / `Phase`. `dependencies()` returns `DependencySpec` and `retry_policy()` returns a value. `Input` and `Output` replace JSON-only input and `TaskOutcome`; task bodies return `(Output, Option<Error>)`. Use `results()` for task summaries, `success_completion_order()` for successful completions, and pass a `Context` plus an option slice to `cancel`.

## Runnable examples

Run from `example/`:

| Command | Scenario |
| --- | --- |
| `cargo run` | Successful DAG |
| `cargo run --bin branching` | Switch, failure handler and `AnyOf` join |
| `cargo run --bin validation` | Graph validation, cascade skip and partial results |
| `cargo run --bin retry_hooks` | Retry, hooks, output with error and task panic |
| `cargo run --bin task_control` | Independent suspension gates and cancellation causes |
| `cargo run --bin observe` | Concurrency limit, progress and observer panic |
| `cargo run --bin cancel_grace` | Cooperative cancellation and grace expiry |

The panic demonstrations intentionally print Rust's panic-hook message before the scheduler catches the panic.

Licensed under [Apache-2.0](LICENSE).
