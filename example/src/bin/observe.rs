use async_trait::async_trait;
use rs_dagcuter::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

struct Step {
    name: String,
    gate: Option<Arc<Semaphore>>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    started: Arc<AtomicUsize>,
}

#[async_trait]
impl Task for Step {
    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(
        &self,
        _ctx: TaskContext,
        _attempt: i64,
        _input: &Input,
    ) -> (Output, Option<Error>) {
        let count = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(count, Ordering::SeqCst);
        self.started.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.gate {
            gate.acquire().await.expect("gate remains open").forget();
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
        (
            HashMap::from([("task".into(), Arc::new(self.name.clone()) as Value)]).into(),
            None,
        )
    }
}

fn step(
    name: &str,
    gate: Option<Arc<Semaphore>>,
    active: &Arc<AtomicUsize>,
    peak: &Arc<AtomicUsize>,
    started: &Arc<AtomicUsize>,
) -> BoxTask {
    Arc::new(Step {
        name: name.into(),
        gate,
        active: active.clone(),
        peak: peak.clone(),
        started: started.clone(),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let events = Arc::new(Mutex::new(Vec::<Event>::new()));
    let received = events.clone();
    let tasks: HashMap<String, BoxTask> = (0..3)
        .map(|index| {
            let name = format!("job-{index}");
            (
                name.clone(),
                step(&name, Some(gate.clone()), &active, &peak, &started),
            )
        })
        .collect();
    let options = [
        with_max_concurrency(2),
        with_observer(move |event| {
            received.lock().unwrap().push(event);
        }),
    ];
    let dag = Arc::new(Scheduler::new(tasks, options)?);
    let runner = dag.clone();
    let handle = tokio::spawn(async move { runner.execute(Context::new()).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while started.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let during = dag.progress();
    assert_eq!(started.load(Ordering::SeqCst), 2);
    assert_eq!((during.total, during.pending, during.done()), (3, 3, 0));
    println!("During execution: {during}");
    gate.add_permits(3);
    let (results, err) = tokio::time::timeout(Duration::from_secs(2), handle).await??;
    assert!(err.is_none());
    assert_eq!(results.len(), 3);
    assert_eq!(peak.load(Ordering::SeqCst), 2);
    assert_eq!(events.lock().unwrap().len(), 3);
    println!(
        "Completed: {}, peak concurrency={}",
        dag.progress(),
        peak.load(Ordering::SeqCst)
    );

    let panic_task = step("observed", None, &active, &peak, &started);
    let dag = Scheduler::new(
        HashMap::from([("observed".into(), panic_task)]),
        [with_observer(|_| panic!("expected observer panic"))],
    )?;
    println!("One observer panic is intentional; it does not change the successful task state.");
    let (outputs, error) = dag.execute(Context::new()).await;
    let error = error.expect("observer panics are reported");
    assert_eq!(dag.phase(), Phase::Success);
    assert_eq!(dag.state("observed"), State::Success);
    assert!(error.failures.is_empty());
    assert_eq!(error.observer_failures.len(), 1);
    assert!(outputs.contains_key("observed"));
    println!(
        "Observer panic reported separately: {}",
        error.observer_failures[0].error
    );
    Ok(())
}
