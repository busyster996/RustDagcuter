use async_trait::async_trait;
use rs_dagcuter::*;
use std::collections::HashMap;
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

    fn dependencies(&self) -> DependencySpec {
        after(self.deps.iter().map(String::as_str))
    }

    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy {
            max_attempts: 3,
            ..Default::default()
        }
    }

    async fn execute(
        &self,
        _ctx: TaskContext,
        _attempt: i64,
        _input: &Input,
    ) -> (Output, Option<Error>) {
        println!("执行任务: {}", self.name);

        // 模拟任务执行时间
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut result = HashMap::new();
        result.insert("status".to_string(), Arc::new("completed") as Value);
        result.insert(
            "task_name".to_string(),
            Arc::new(self.name.clone()) as Value,
        );
        result.insert(
            "timestamp".to_string(),
            Arc::new(chrono::Utc::now().to_rfc3339()),
        );
        (result.into(), None)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tasks: HashMap<String, BoxTask> = HashMap::new();

    tasks.insert(
        "task1".to_string(),
        Arc::new(ExampleTask {
            name: "task1".to_string(),
            deps: vec![],
        }),
    );

    tasks.insert(
        "task2".to_string(),
        Arc::new(ExampleTask {
            name: "task2".to_string(),
            deps: vec!["task1".to_string()],
        }),
    );

    tasks.insert(
        "task3".to_string(),
        Arc::new(ExampleTask {
            name: "task3".to_string(),
            deps: vec!["task1".to_string()],
        }),
    );

    tasks.insert(
        "task4".to_string(),
        Arc::new(ExampleTask {
            name: "task4".to_string(),
            deps: vec!["task2".to_string(), "task3".to_string()],
        }),
    );

    tasks.insert(
        "task5".to_string(),
        Arc::new(ExampleTask {
            name: "task5".to_string(),
            deps: vec!["task2".to_string()],
        }),
    );

    tasks.insert(
        "task6".to_string(),
        Arc::new(ExampleTask {
            name: "task6".to_string(),
            deps: vec![
                "task1".to_string(),
                "task4".to_string(),
                "task5".to_string(),
            ],
        }),
    );

    let dag = Scheduler::new(tasks, [])?;
    let ctx = Context::new();

    println!("=== 任务依赖图 ===");
    dag.print_graph();

    println!("=== 开始执行任务 ===");
    let start = std::time::Instant::now();
    let (results, err) = dag.execute(ctx).await;
    if let Some(err) = err {
        return Err(err.into());
    }
    let duration = start.elapsed();

    println!("=== 执行完成 ===");
    println!("执行时间: {:?}", duration);
    println!("执行结果: {:#?}", results);
    println!("执行顺序: {}", dag.success_completion_order());

    Ok(())
}
