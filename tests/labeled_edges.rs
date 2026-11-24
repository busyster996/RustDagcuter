use async_trait::async_trait;
use rs_dagcuter::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

struct Step<F> {
    name: &'static str,
    spec: DependencySpec,
    run: F,
}

#[async_trait]
impl<F: Fn(&Input) -> (Output, Option<Error>) + Send + Sync> Task for Step<F> {
    fn name(&self) -> &str {
        self.name
    }

    fn dependencies(&self) -> DependencySpec {
        self.spec.clone()
    }

    async fn execute(&self, _: TaskContext, _: i64, input: &Input) -> (Output, Option<Error>) {
        (self.run)(input)
    }
}

fn step(
    name: &'static str,
    spec: DependencySpec,
    run: impl Fn(&Input) -> (Output, Option<Error>) + Send + Sync + 'static,
) -> (String, BoxTask) {
    (name.into(), Arc::new(Step { name, spec, run }))
}

fn spec(join: JoinPolicy, edges: &[(&str, &str)]) -> DependencySpec {
    DependencySpec {
        join,
        edges: edges
            .iter()
            .map(|&(task, on)| DependencyEdge::new(task, on))
            .collect(),
    }
}

fn success(_: &Input) -> (Output, Option<Error>) {
    (Data::new().into(), None)
}

async fn execute(dag: &Scheduler) -> (Outputs, Option<RunError>) {
    tokio::time::timeout(Duration::from_secs(5), dag.execute(Context::new()))
        .await
        .expect("DAG did not settle")
}

fn successful((outputs, error): (Outputs, Option<RunError>)) -> Outputs {
    assert!(error.is_none(), "{error:?}");
    outputs
}

#[tokio::test]
async fn switch_selects_only_matching_edges_and_any_of_joins() {
    let dag = Scheduler::new(
        HashMap::from([
            step("switch", DependencySpec::default(), |_| {
                (
                    Output {
                        data: HashMap::from([("choice".into(), Arc::new("blue") as Value)]),
                        emit: vec!["blue".into()],
                    },
                    None,
                )
            }),
            step(
                "blue",
                spec(JoinPolicy::AllOf, &[("switch", "blue")]),
                |input| {
                    assert_eq!(
                        input.outputs["switch"]["choice"]
                            .downcast_ref::<&str>()
                            .copied(),
                        Some("blue")
                    );
                    assert!(input.errors.is_empty());
                    success(input)
                },
            ),
            step(
                "red",
                spec(JoinPolicy::AllOf, &[("switch", "Blue")]),
                success,
            ),
            step("default", after(["switch"]), success),
            step(
                "join",
                spec(JoinPolicy::AnyOf, &[("blue", ""), ("red", "")]),
                |input| {
                    assert_eq!(input.outputs.len(), 1);
                    assert!(input.outputs.contains_key("blue"));
                    success(input)
                },
            ),
            step("all", after(["blue", "red"]), success),
        ]),
        [],
    )
    .unwrap();

    let output = successful(execute(&dag).await);
    assert_eq!(output.len(), 3);
    for skipped in ["red", "default", "all"] {
        assert_eq!(dag.state(skipped), State::Skipped);
    }
    assert_eq!(dag.phase(), Phase::Success);
    assert_eq!(
        dag.success_completion_order(),
        "\n1. switch\n2. blue\n3. join\n"
    );
}

#[tokio::test]
async fn failure_handler_sees_original_error_and_does_not_clear_failure() {
    let dag = Scheduler::new(
        HashMap::from([
            step("up", DependencySpec::default(), |_| {
                (
                    Data::new().into(),
                    Some(Error::TaskExecution("boom".into())),
                )
            }),
            step("normal", after(["up"]), success),
            step(
                "handler",
                spec(JoinPolicy::AnyOf, &[("up", ON_SUCCESS), ("up", ON_FAILURE)]),
                |input| {
                    assert!(input.outputs.is_empty());
                    assert!(input.errors.contains_key("up"));
                    success(input)
                },
            ),
            step(
                "skipped_handler",
                spec(JoinPolicy::AllOf, &[("normal", ON_FAILURE)]),
                success,
            ),
        ]),
        [],
    )
    .unwrap();

    let (outputs, error) = execute(&dag).await;
    let error = error.unwrap();
    assert_eq!(error.failures.len(), 1);
    assert_eq!(error.failures[0].task, "up");
    assert_eq!(outputs.len(), 1);
    assert!(outputs.contains_key("handler"));
    assert_eq!(dag.phase(), Phase::Failed);
    assert_eq!(dag.state("normal"), State::Skipped);
    assert_eq!(dag.state("skipped_handler"), State::Skipped);
    let mut graph = Vec::new();
    dag.write_graph(&mut graph).unwrap();
    let graph = String::from_utf8(graph).unwrap();
    assert!(graph.contains("handler [on failure] (any-of)\n  └─> handler (any-of) ..."));
}

#[tokio::test]
async fn empty_labels_are_success_and_parallel_edges_count_individually() {
    let dag = Scheduler::new(
        HashMap::from([
            step("up", spec(JoinPolicy::AnyOf, &[]), |_| {
                (
                    Output {
                        data: Data::new(),
                        emit: vec!["".into(), "extra".into(), "extra".into()],
                    },
                    None,
                )
            }),
            step(
                "down",
                spec(
                    JoinPolicy::AllOf,
                    &[("up", ""), ("up", ON_SUCCESS), ("up", "extra")],
                ),
                |input| {
                    assert_eq!(input.outputs.len(), 1);
                    assert!(input.outputs.contains_key("up"));
                    success(input)
                },
            ),
        ]),
        [],
    )
    .unwrap();
    assert_eq!(successful(execute(&dag).await).len(), 2);
    assert_eq!(dag.results()["down"].attempts, 1);
}

struct WaitForCancel(Arc<Notify>);

#[async_trait]
impl Task for WaitForCancel {
    fn name(&self) -> &str {
        "wait"
    }

    async fn execute(&self, ctx: TaskContext, _: i64, _: &Input) -> (Output, Option<Error>) {
        self.0.notify_one();
        ctx.cancelled().await;
        (
            Output::default(),
            Some(Error::ContextCancelled("stopped".into())),
        )
    }
}

#[tokio::test]
async fn any_of_waits_for_all_sources_and_propagates_cancellation() {
    let fast_done = Arc::new(Notify::new());
    let wait_started = Arc::new(Notify::new());
    let dag = Arc::new(
        Scheduler::new(
            HashMap::from([
                (
                    "wait".into(),
                    Arc::new(WaitForCancel(wait_started.clone())) as BoxTask,
                ),
                step("fast", DependencySpec::default(), success),
                step(
                    "join",
                    spec(JoinPolicy::AnyOf, &[("fast", ""), ("wait", "custom")]),
                    success,
                ),
                step(
                    "handler",
                    spec(JoinPolicy::AllOf, &[("wait", ON_FAILURE)]),
                    success,
                ),
            ]),
            [with_observer({
                let fast_done = fast_done.clone();
                move |event| {
                    if event.task == "fast" {
                        fast_done.notify_one();
                    }
                }
            })],
        )
        .unwrap(),
    );
    let run = tokio::spawn({
        let dag = dag.clone();
        async move { execute(&dag).await }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        fast_done.notified().await;
        wait_started.notified().await;
    })
    .await
    .unwrap();
    assert_eq!(dag.state("join"), State::Pending);
    dag.cancel_task("wait", &[]).unwrap();
    let (outputs, error) = run.await.unwrap();
    let error = error.unwrap();
    assert_eq!(error.failures.len(), 1);
    assert_eq!(outputs.len(), 1);
    assert_eq!(dag.phase(), Phase::Canceled);
    for name in ["wait", "join", "handler"] {
        assert_eq!(dag.state(name), State::Canceled);
    }
}

struct InvalidEmit;

#[async_trait]
impl Task for InvalidEmit {
    fn name(&self) -> &str {
        "invalid"
    }
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy {
            max_attempts: 3,
            interval: Duration::from_millis(1),
            ..Default::default()
        }
    }

    async fn execute(&self, _: TaskContext, _: i64, _: &Input) -> (Output, Option<Error>) {
        (
            Output {
                data: HashMap::from([("discard".into(), Arc::new(true) as Value)]),
                emit: vec![ON_FAILURE.into()],
            },
            None,
        )
    }

    async fn post_execution(&self, _: TaskContext, _: i64, output: &Output, error: Option<&Error>) {
        assert_eq!(output.emit, [ON_FAILURE]);
        assert!(error.is_none());
    }
}

#[tokio::test]
async fn reserved_failure_emission_is_rejected_after_hooks_and_never_retried() {
    let dag = Scheduler::new(
        HashMap::from([
            ("invalid".into(), Arc::new(InvalidEmit) as BoxTask),
            step(
                "handler",
                spec(JoinPolicy::AllOf, &[("invalid", ON_FAILURE)]),
                |input| {
                    assert!(matches!(
                        input.errors["invalid"].as_ref(),
                        Error::InvalidEmit { .. }
                    ));
                    success(input)
                },
            ),
        ]),
        [],
    )
    .unwrap();
    let (outputs, error) = execute(&dag).await;
    let error = error.unwrap();
    assert_eq!(dag.results()["invalid"].attempts, 1);
    assert!(matches!(
        error.failures[0].error.as_ref(),
        Error::InvalidEmit { .. }
    ));
    assert!(outputs.contains_key("handler"));
    assert!(!outputs.contains_key("invalid"));
}

#[test]
fn validation_uses_all_edges_regardless_of_label_or_join() {
    let tasks = HashMap::from([step(
        "one",
        spec(JoinPolicy::AnyOf, &[("absent", "custom")]),
        success,
    )]);
    assert!(matches!(
        validate(&tasks),
        Err(Error::UnknownDependency { .. })
    ));
    let tasks = HashMap::from([
        step(
            "one",
            spec(JoinPolicy::AnyOf, &[("two", "custom")]),
            success,
        ),
        step(
            "two",
            spec(JoinPolicy::AllOf, &[("one", ON_FAILURE)]),
            success,
        ),
    ]);
    assert!(matches!(
        validate(&tasks),
        Err(Error::CircularDependency(_))
    ));
}

struct ChangingDependencies(std::sync::atomic::AtomicUsize);

#[async_trait]
impl Task for ChangingDependencies {
    fn name(&self) -> &str {
        "child"
    }

    fn dependencies(&self) -> DependencySpec {
        use std::sync::atomic::Ordering;
        if self.0.fetch_add(1, Ordering::Relaxed) == 0 {
            spec(JoinPolicy::AllOf, &[("root", "selected")])
        } else {
            after(["missing"])
        }
    }

    async fn execute(&self, _: TaskContext, _: i64, input: &Input) -> (Output, Option<Error>) {
        assert!(input.outputs.contains_key("root"));
        success(input)
    }
}

#[tokio::test]
async fn scheduling_and_graph_share_the_frozen_edge_snapshot() {
    let child = Arc::new(ChangingDependencies(0.into()));
    let dag = Scheduler::new(
        HashMap::from([
            ("child".into(), child.clone() as BoxTask),
            step("root", DependencySpec::default(), |_| {
                (
                    Output {
                        emit: vec!["selected".into()],
                        ..Output::default()
                    },
                    None,
                )
            }),
        ]),
        [],
    )
    .unwrap();
    let mut before = Vec::new();
    dag.write_graph(&mut before).unwrap();
    assert_eq!(successful(execute(&dag).await).len(), 2);
    let mut after = Vec::new();
    dag.write_graph(&mut after).unwrap();
    assert_eq!(before, after);
    assert_eq!(
        String::from_utf8(after).unwrap(),
        "root\n  └─> child [on selected]\n\n"
    );
    assert_eq!(child.0.load(std::sync::atomic::Ordering::Relaxed), 1);
}
