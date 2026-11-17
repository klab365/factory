//! Ports used by application use cases to execute pipeline steps.

use crate::{AgentDefinition, Step};

/// A structured result returned by a step runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepOutput {
    pub outcome: String,
    pub output: String,
    pub exit_code: Option<i32>,
}

/// Port for executing an agent step. Implementations belong in adapters.
pub trait AgentRunner {
    fn run(&mut self, agent: &AgentDefinition, step: &Step) -> Result<StepOutput, String>;
}

/// Port for executing a shell step. Implementations belong in adapters.
pub trait ShellRunner {
    fn run(&mut self, step: &Step) -> Result<StepOutput, String>;
}
