//! Adapter boundary for harness-specific agent execution.

use factory_core::features::{AgentRunner, StepOutput, StepResult};
use factory_core::{AgentDefinition, Step, StepKind};

/// Backend contract implemented by a concrete agent harness integration.
pub trait AgentExecutor {
    fn execute(
        &mut self,
        agent: &AgentDefinition,
        step: &Step,
        previous_result: Option<&StepResult>,
    ) -> Result<StepOutput, String>;
}

/// Bridges the application port to a harness executor without exposing
/// harness-specific types to factory-core's application features.
pub struct AgentRunnerAdapter<E> {
    executor: E,
}

impl<E> AgentRunnerAdapter<E> {
    pub fn new(executor: E) -> Self {
        Self { executor }
    }

    pub fn into_inner(self) -> E {
        self.executor
    }
}

impl<E: AgentExecutor> AgentRunner for AgentRunnerAdapter<E> {
    fn run(
        &mut self,
        agent: &AgentDefinition,
        step: &Step,
        previous_result: Option<&StepResult>,
    ) -> Result<StepOutput, String> {
        match &step.kind {
            StepKind::Agent { agent: step_agent } if step_agent == &agent.name => {
                self.executor.execute(agent, step, previous_result)
            }
            StepKind::Agent { agent: step_agent } => Err(format!(
                "agent step `{}` references `{step_agent}`, but runner received `{}`",
                step.id, agent.name
            )),
            StepKind::Shell { .. } => Err(format!(
                "agent runner cannot execute shell step `{}`",
                step.id
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use factory_core::{ModelRef, StepKind};

    struct FakeExecutor;

    impl AgentExecutor for FakeExecutor {
        fn execute(
            &mut self,
            _: &AgentDefinition,
            _: &Step,
            _: Option<&StepResult>,
        ) -> Result<StepOutput, String> {
            Ok(StepOutput {
                outcome: "success".into(),
                output: "done".into(),
                exit_code: None,
                attempts: 1,
                handoff: None,
            })
        }
    }

    #[test]
    fn delegates_matching_agent_steps_to_the_harness_executor() {
        let agent = AgentDefinition {
            name: "coder".into(),
            harness: "test".into(),
            model: ModelRef::parse("test/model").unwrap(),
            thinking_level: None,
            prompt: "prompt".into(),
        };
        let step = Step {
            id: "build".into(),
            kind: StepKind::Agent {
                agent: "coder".into(),
            },
            transitions: vec![],
        };
        let mut runner = AgentRunnerAdapter::new(FakeExecutor);
        assert_eq!(runner.run(&agent, &step, None).unwrap().outcome, "success");
    }
}
