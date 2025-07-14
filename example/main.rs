use dagcuter::*;
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
    ) -> Result<TaskResult, DagcuterError> {
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



    let mut dagcuter = Dagcuter::new(tasks)?;
    let ctx = CancellationToken::new();

    println!("=== 任务依赖图 ===");
    dagcuter.print_graph();

    println!("=== 开始执行任务 ===");
    let start = std::time::Instant::now();
    let results = dagcuter.execute(ctx).await?;
    let duration = start.elapsed();

    println!("=== 执行完成 ===");
    println!("执行时间: {:?}", duration);
    println!("执行结果: {:#?}", results);
    println!("执行顺序: {}", dagcuter.execution_order().await);

    Ok(())
}