//! Pipeline run state and execution records.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepHandoff {
    Review(ReviewHandoff),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewHandoff {
    pub findings: Vec<ReviewFinding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewFinding {
    pub title: String,
    pub location: String,
    pub problem: String,
    pub required_change: String,
    pub verification: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepResult {
    pub step_id: String,
    pub outcome: String,
    pub output: String,
    pub exit_code: Option<i32>,
    pub runner_attempts: u32,
    pub handoff: Option<StepHandoff>,
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
