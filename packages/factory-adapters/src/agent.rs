//! Adapter boundary for harness-specific agent execution.

use std::path::Path;

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

    fn execute_with_cancel(
        &mut self,
        agent: &AgentDefinition,
        step: &Step,
        previous_result: Option<&StepResult>,
        _cancelled: &dyn Fn() -> bool,
    ) -> Result<StepOutput, String> {
        self.execute(agent, step, previous_result)
    }

    /// Run memory used by the shared lifecycle to persist incoming handoffs.
    fn run_memory_path(&self, _agent: &AgentDefinition) -> Option<&Path> {
        None
    }

    fn execute_with_handoff(
        &mut self,
        agent: &AgentDefinition,
        step: &Step,
        previous_result: Option<&StepResult>,
        _handoff: Option<&str>,
        cancelled: &dyn Fn() -> bool,
    ) -> Result<StepOutput, String> {
        self.execute_with_cancel(agent, step, previous_result, cancelled)
    }
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
        self.run_with_cancel(agent, step, previous_result, &|| false)
    }

    fn run_with_cancel(
        &mut self,
        agent: &AgentDefinition,
        step: &Step,
        previous_result: Option<&StepResult>,
        cancelled: &dyn Fn() -> bool,
    ) -> Result<StepOutput, String> {
        match &step.kind {
            StepKind::Agent {
                agent: step_agent, ..
            } if step_agent == &agent.name => {
                let handoff = self
                    .executor
                    .run_memory_path(agent)
                    .map(|memory| {
                        crate::run_memory::record_handoff(
                            memory,
                            &agent.name,
                            &step.id,
                            previous_result,
                        )
                    })
                    .transpose()?;
                self.executor.execute_with_handoff(
                    agent,
                    step,
                    previous_result,
                    handoff.as_deref(),
                    cancelled,
                )
            }
            StepKind::Agent {
                agent: step_agent, ..
            } => Err(format!(
                "agent step `{}` references `{step_agent}`, but runner received `{}`",
                step.id, agent.name
            )),
            StepKind::Shell { .. } | StepKind::Human { .. } => Err(format!(
                "agent runner cannot execute non-agent step `{}`",
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

    struct MemoryExecutor(std::path::PathBuf);

    impl AgentExecutor for MemoryExecutor {
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

        fn run_memory_path(&self, _: &AgentDefinition) -> Option<&Path> {
            Some(&self.0)
        }
    }

    #[test]
    fn shared_lifecycle_records_review_handoff_for_non_pi_executor() {
        use factory_core::features::{ReviewFinding, ReviewHandoff, StepHandoff};

        let dir = std::env::temp_dir().join(format!("agent-run-memory-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let memory = dir.join("coder.md");
        std::fs::write(&memory, "# Run memory: coder\n").unwrap();
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
                context: vec![],
            },
            transitions: vec![],
        };
        let previous = StepResult {
            step_id: "review".into(),
            outcome: "changes_requested".into(),
            output: "summary".into(),
            exit_code: Some(0),
            runner_attempts: 1,
            handoff: Some(StepHandoff::Review(ReviewHandoff {
                findings: vec![ReviewFinding {
                    title: "General lifecycle handling".into(),
                    location: "agent.rs".into(),
                    problem: "Pi-only handling".into(),
                    required_change: "Move to shared lifecycle".into(),
                    verification: "Test another executor".into(),
                }],
            })),
        };

        let mut runner = AgentRunnerAdapter::new(MemoryExecutor(memory.clone()));
        runner.run(&agent, &step, Some(&previous)).unwrap();
        let saved = std::fs::read_to_string(&memory).unwrap();
        assert!(saved.contains("## 1. Handoff: Step `review` → Agent `coder` / Step `build`"));
        assert!(saved.contains("### 1.1 General lifecycle handling"));
        assert_eq!(saved.matches("## 1. Handoff:").count(), 1);
        std::fs::remove_dir_all(dir).unwrap();
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
                context: vec![],
            },
            transitions: vec![],
        };
        let mut runner = AgentRunnerAdapter::new(FakeExecutor);
        assert_eq!(runner.run(&agent, &step, None).unwrap().outcome, "success");
    }
}
