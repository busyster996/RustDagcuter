use crate::{BoxTask, Error, Retry, TaskInput, TaskResult};
use futures::future::try_join_all;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

/// Detects cycles in the task dependency graph using Depth-First Search (DFS)
/// This is crucial to prevent infinite loops during execution
/// Returns true if a cycle is detected, false otherwise
fn has_cycle(tasks: &HashMap<String, BoxTask>) -> bool {
    let mut visited = HashSet::new(); // Tracks all visited nodes
    let mut rec_stack = HashSet::new(); // Tracks nodes in current recursion path

    /// DFS helper function that traverses the dependency graph
    /// Uses recursion stack to detect back edges (cycles)
    fn dfs(
        task_name: &str,
        tasks: &HashMap<String, BoxTask>,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
    ) -> bool {
        // If current node is in recursion stack, we found a back edge (cycle)
        if rec_stack.contains(task_name) {
            return true; // Cycle detected
        }
        // If already processed this node, no need to check again
        if visited.contains(task_name) {
            return false; // Already processed
        }

        // Mark current node as visited and add to recursion stack
        visited.insert(task_name.to_string());
        rec_stack.insert(task_name.to_string());

        // Recursively check all dependencies of current task
        if let Some(task) = tasks.get(task_name) {
            for dep in task.dependencies() {
                if dfs(&dep, tasks, visited, rec_stack) {
                    return true; // Cycle found in dependency
                }
            }
        }

        // Remove from recursion stack when backtracking
        rec_stack.remove(task_name);
        false
    }

    // Check each task as potential starting point for cycle detection
    for task_name in tasks.keys() {
        if !visited.contains(task_name) && dfs(task_name, tasks, &mut visited, &mut rec_stack) {
            return true;
        }
    }

    false
}

/// DAG (Directed Acyclic Graph) Executor for task scheduling
/// Manages task dependencies and executes them in topological order
/// Uses Kahn's algorithm for topological sorting with concurrent execution
pub struct Dag {
    /// All tasks indexed by their names
    tasks: HashMap<String, BoxTask>,
    /// Shared storage for task execution results, protected by RwLock for concurrent access
    results: Arc<RwLock<HashMap<String, TaskResult>>>,
    /// In-degree count for each task (number of dependencies)
    /// Used for topological sorting - tasks with 0 in-degree can be executed
    in_degrees: HashMap<String, i32>,
    /// Maps each task to its dependent tasks (reverse dependency)
    /// Used to update in-degrees when a task completes
    dependents: HashMap<String, Vec<String>>,
    /// Records the actual execution order for debugging and monitoring
    execution_order: Arc<Mutex<Vec<String>>>,
}

impl Dag {
    /// Creates a new DAG executor from a collection of tasks
    /// Validates that the dependency graph is acyclic and builds internal structures
    pub fn new(tasks: HashMap<String, BoxTask>) -> Result<Self, Error> {
        // Prevent infinite loops by checking for circular dependencies
        if has_cycle(&tasks) {
            return Err(Error::CircularDependency);
        }

        let mut in_degrees = HashMap::new();
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();

        // Build topological sorting data structures
        // Calculate in-degree (dependency count) for each task
        // Build reverse dependency mapping for efficient updates
        for (name, task) in &tasks {
            in_degrees.insert(name.clone(), task.dependencies().len() as i32);
            for dep in task.dependencies() {
                dependents
                    .entry(dep)
                    .or_insert_with(Vec::new)
                    .push(name.clone());
            }
        }

        Ok(Self {
            tasks,
            results: Arc::new(RwLock::new(HashMap::new())),
            in_degrees,
            dependents,
            execution_order: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Executes all tasks in dependency order with support for cancellation
    /// Uses modified Kahn's algorithm with concurrent task execution
    /// Returns all task results or an error if execution fails
    pub async fn execute(
        &mut self,
        ctx: CancellationToken,
    ) -> Result<HashMap<String, TaskResult>, Error> {
        // Reset state for fresh execution
        self.results.write().await.clear();
        self.execution_order.lock().await.clear();

        // Communication channels for task scheduling
        let (task_tx, mut task_rx) = mpsc::unbounded_channel::<String>(); // Ready tasks
        let (completion_tx, mut completion_rx) = mpsc::unbounded_channel::<String>(); // Completed tasks
        let in_degrees = Arc::new(Mutex::new(self.in_degrees.clone()));
        let mut remaining_tasks = self.tasks.len();

        // Initialize with tasks that have no dependencies (in-degree = 0)
        // These can be executed immediately
        {
            let degrees = in_degrees.lock().await;
            for (name, &degree) in degrees.iter() {
                if degree == 0 {
                    task_tx.send(name.clone()).map_err(|_| {
                        Error::TaskExecution("Failed to send initial task".to_string())
                    })?;
                }
            }
        }

        // Semaphore to limit concurrent task execution and prevent resource exhaustion
        let semaphore = Arc::new(Semaphore::new(1024));
        let mut handles = Vec::new();

        // Main execution loop using Kahn's algorithm
        // Processes tasks as their dependencies are satisfied
        while remaining_tasks > 0 {
            tokio::select! {
                // New task is ready to execute (all dependencies satisfied)
                Some(task_name) = task_rx.recv() => {
                    // Acquire permit to control concurrency
                    let permit = semaphore.clone().acquire_owned().await.map_err(|_| {
                        Error::TaskExecution("Failed to acquire semaphore".to_string())
                    })?;

                    // Spawn task execution in background
                    let handle = self.spawn_task(
                        ctx.clone(),
                        task_name,
                        completion_tx.clone(),
                        permit,
                    ).await;
                    handles.push(handle);
                }

                // Task completed, update dependency graph
                Some(completed_task) = completion_rx.recv() => {
                    remaining_tasks -= 1;
                    // Decrease in-degree for all dependent tasks
                    // If any task reaches 0 in-degree, it's ready to execute
                    if let Some(children) = self.dependents.get(&completed_task) {
                        let mut degrees = in_degrees.lock().await;
                        for child in children {
                            if let Some(degree) = degrees.get_mut(child) {
                                *degree -= 1;
                                if *degree == 0 {
                                    // Task is now ready (all dependencies satisfied)
                                    task_tx.send(child.clone()).map_err(|_| {
                                        Error::TaskExecution("Failed to send child task".to_string())
                                    })?;
                                }
                            }
                        }
                    }
                }

                // Handle cancellation request
                _ = ctx.cancelled() => {
                    return Err(Error::ContextCancelled("Execution cancelled".to_string()));
                }
            }
        }

        // Wait for all spawned tasks to complete
        try_join_all(handles)
            .await
            .map_err(|e| Error::TaskExecution(format!("Join error: {}", e)))?;

        Ok(self.results.read().await.clone())
    }

    /// Spawns a single task execution in a separate async task
    /// Handles task preparation, execution, and result storage
    /// Returns a JoinHandle for the spawned task
    async fn spawn_task(
        &self,
        ctx: CancellationToken,
        task_name: String,
        completion_tx: mpsc::UnboundedSender<String>,
        _concurrency_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> tokio::task::JoinHandle<Result<(), Error>> {
        let task = self.tasks.get(&task_name).unwrap().clone();
        let results = Arc::clone(&self.results);
        let execution_order = Arc::clone(&self.execution_order);

        tokio::spawn(async move {
            // Prepare inputs from dependency results
            let inputs = Self::prepare_inputs(&task, &results).await;
            // Execute the task with retry logic
            let output = Self::execute_task(ctx, &task_name, &task, inputs).await?;

            // Record execution order and store results
            execution_order.lock().await.push(task_name.clone());
            results.write().await.insert(task_name.clone(), output);

            // Signal completion to the main loop
            completion_tx.send(task_name).map_err(|_| {
                Error::TaskExecution("Failed to send completion signal".to_string())
            })?;

            Ok(())
        })
    }

    /// Prepares input data for a task by collecting results from its dependencies
    /// Creates a HashMap mapping dependency names to their output values
    async fn prepare_inputs(
        task: &BoxTask,
        results: &Arc<RwLock<HashMap<String, TaskResult>>>,
    ) -> TaskInput {
        let mut inputs = HashMap::new();
        let results_read = results.read().await;

        // Collect outputs from all dependency tasks
        for dep in task.dependencies() {
            if let Some(value) = results_read.get(&dep) {
                // Convert task result to JSON value for flexible input handling
                inputs.insert(dep, serde_json::to_value(value).unwrap());
            }
        }
        inputs
    }

    /// Executes a single task with retry logic and lifecycle hooks
    /// Handles pre-execution, main execution, and post-execution phases
    async fn execute_task(
        ctx: CancellationToken,
        name: &str,
        task: &BoxTask,
        inputs: TaskInput,
    ) -> Result<TaskResult, Error> {
        let retry = Retry::new(task.retry_policy());

        // Execute with retry mechanism
        retry
            .execute_with_retry(ctx.clone(), name, |attempt| {
                let ctx = ctx.clone();
                let task = task.clone();
                let mut task_inputs = inputs.clone();

                async move {
                    // Add attempt number to inputs for task awareness
                    task_inputs.insert("attempt".to_string(), serde_json::json!(attempt));

                    // Execute task lifecycle: pre -> main -> post
                    task.pre_execution(ctx.clone(), &task_inputs).await?;
                    let output = task.execute(ctx.clone(), &task_inputs).await?;
                    task.post_execution(ctx, &output).await?;
                    Ok(output)
                }
            })
            .await
    }

    /// Returns a formatted string showing the actual execution order
    /// Useful for debugging and monitoring task execution flow
    pub async fn execution_order(&self) -> String {
        let order = self.execution_order.lock().await;
        let mut result = String::from("\n");
        for (i, step) in order.iter().enumerate() {
            result.push_str(&format!("{}. {}\n", i + 1, step));
        }
        result
    }

    /// Prints the dependency graph structure to console
    /// Shows the hierarchical relationship between tasks
    pub fn print_graph(&self) {
        // Find root tasks (no dependencies)
        let mut roots = Vec::new();
        for (name, &degree) in &self.in_degrees {
            if degree == 0 {
                roots.push(name.clone());
            }
        }

        // Print each root and its dependency chain
        for root in roots {
            println!("{}", root);
            self.print_chain(&root, "  ");
            println!();
        }
    }

    /// Recursively prints the dependency chain starting from a given task
    /// Uses tree-like formatting to show hierarchical relationships
    fn print_chain(&self, name: &str, prefix: &str) {
        if let Some(children) = self.dependents.get(name) {
            for child in children {
                println!("{}└─> {}", prefix, child);
                // Recursive call with increased indentation
                self.print_chain(child, &format!("{}    ", prefix));
            }
        }
    }
}
