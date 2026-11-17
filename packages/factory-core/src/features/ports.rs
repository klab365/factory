//! Ports used by application use cases to execute pipeline steps.

use crate::{AgentDefinition, Step};

use super::run::{StepHandoff, StepResult};

/// A structured result returned by a step runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepOutput {
    pub outcome: String,
    pub output: String,
    pub exit_code: Option<i32>,
    /// Number of adapter-level attempts used to produce this result.
    pub attempts: u32,
    pub handoff: Option<StepHandoff>,
}

/// Port for executing an agent step. Implementations belong in adapters.
pub trait AgentRunner {
    /// Executes an agent step with the immediately preceding pipeline result, if any.
    fn run(
        &mut self,
        agent: &AgentDefinition,
        step: &Step,
        previous_result: Option<&StepResult>,
    ) -> Result<StepOutput, String>;
}

/// Port for executing a shell step. Implementations belong in adapters.
pub trait ShellRunner {
    fn run(&mut self, step: &Step) -> Result<StepOutput, String>;
}

/// Persistence port called before and after each step execution.
pub trait RunJournal {
    fn step_started(&mut self, step_id: &str) -> Result<(), String>;
    fn step_completed(&mut self, result: &StepResult) -> Result<(), String>;
    fn step_failed(&mut self, step_id: &str, error: &str) -> Result<(), String>;

    /// Called when a reviewer repeats an identical structured rejection. Return false to stop.
    fn confirm_repeated_review_feedback(&mut self, _result: &StepResult) -> Result<bool, String> {
        Ok(true)
    }
}
