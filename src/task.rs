use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use crate::{RetryPolicy, TaskResult, TaskInput, DagcuterError};

#[async_trait]
pub trait Task: Send + Sync {
    fn name(&self) -> &str;
    fn dependencies(&self) -> Vec<String>;
    fn retry_policy(&self) -> Option<RetryPolicy>;

    async fn pre_execution(
        &self,
        _ctx: CancellationToken,
        _input: &TaskInput,
    ) -> Result<(), DagcuterError> {
        Ok(())
    }

    async fn execute(
        &self,
        ctx: CancellationToken,
        input: &TaskInput,
    ) -> Result<TaskResult, DagcuterError>;

    async fn post_execution(
        &self,
        _ctx: CancellationToken,
        _output: &TaskResult,
    ) -> Result<(), DagcuterError> {
        Ok(())
    }
}

