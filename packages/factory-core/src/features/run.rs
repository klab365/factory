//! Pipeline run state and execution records.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepResult {
    pub step_id: String,
    pub outcome: String,
    pub output: String,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReport {
    pub pipeline_name: String,
    pub status: RunStatus,
    pub steps: Vec<StepResult>,
}
