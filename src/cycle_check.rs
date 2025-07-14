use std::collections::{HashMap, HashSet};
use crate::BoxTask;

pub fn has_cycle(tasks: &HashMap<String, BoxTask>) -> bool {
    let mut visited = HashSet::new();
    let mut rec_stack = HashSet::new();

    fn dfs(
        task_name: &str,
        tasks: &HashMap<String, BoxTask>,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
    ) -> bool {
        if rec_stack.contains(task_name) {
            return true; // 检测到循环
        }
        if visited.contains(task_name) {
            return false; // 已处理
        }

        visited.insert(task_name.to_string());
        rec_stack.insert(task_name.to_string());

        if let Some(task) = tasks.get(task_name) {
            for dep in task.dependencies() {
                if dfs(&dep, tasks, visited, rec_stack) {
                    return true;
                }
            }
        }

        rec_stack.remove(task_name);
        false
    }

    for task_name in tasks.keys() {
        if !visited.contains(task_name) {
            if dfs(task_name, tasks, &mut visited, &mut rec_stack) {
                return true;
            }
        }
    }

    false
}

