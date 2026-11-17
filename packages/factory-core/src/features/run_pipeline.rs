//! Executes a pipeline through the application runner ports.

use std::{collections::HashMap, error::Error, fmt};

use crate::{AgentDefinition, Pipeline, Step, StepKind};

use super::{
    ports::{AgentRunner, RunJournal, ShellRunner},
    run::{RunReport, RunStatus, StepResult},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunError {
    EmptyPipeline,
    MissingAgent { step_id: String, agent: String },
    RunnerFailed { step_id: String, message: String },
    UnknownOutcome { step_id: String, outcome: String },
    AmbiguousOutcome { step_id: String, outcome: String },
    MissingTarget { step_id: String, target: String },
    StepLimitExceeded { limit: usize },
    JournalFailed { message: String },
    InvalidResumeState { message: String },
    InvalidStepKind { step_id: String },
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPipeline => write!(f, "cannot run an empty pipeline"),
            Self::MissingAgent { step_id, agent } => {
                write!(f, "step `{step_id}` references missing agent `{agent}`")
            }
            Self::RunnerFailed { step_id, message } => {
                write!(f, "step `{step_id}` failed to execute: {message}")
            }
            Self::UnknownOutcome { step_id, outcome } => {
                write!(f, "step `{step_id}` returned unknown outcome `{outcome}`")
            }
            Self::AmbiguousOutcome { step_id, outcome } => {
                write!(
                    f,
                    "step `{step_id}` has multiple transitions for outcome `{outcome}`"
                )
            }
            Self::MissingTarget { step_id, target } => {
                write!(f, "step `{step_id}` transitions to missing step `{target}`")
            }
            Self::StepLimitExceeded { limit } => {
                write!(f, "pipeline exceeded the step execution limit of {limit}")
            }
            Self::JournalFailed { message } => {
                write!(f, "could not persist pipeline run: {message}")
            }
            Self::InvalidResumeState { message } => write!(f, "cannot resume pipeline: {message}"),
            Self::InvalidStepKind { step_id } => {
                write!(f, "step `{step_id}` has an invalid runner kind")
            }
        }
    }
}

impl Error for RunError {}

/// Runs a pipeline from its first step with an in-memory no-op journal.
/// `success` and `failure` are terminal outcomes when no transition handles them.
pub fn run_pipeline<A: AgentRunner, S: ShellRunner>(
    pipeline: &Pipeline,
    agents: &[AgentDefinition],
    agent_runner: &mut A,
    shell_runner: &mut S,
    max_step_executions: usize,
) -> Result<RunReport, RunError> {
    let mut journal = NoopJournal;
    run_pipeline_with_journal(
        pipeline,
        agents,
        agent_runner,
        shell_runner,
        &[],
        &mut journal,
        max_step_executions,
    )
}

/// Resumes from persisted completed results and journals each execution attempt.
pub fn run_pipeline_with_journal<A: AgentRunner, S: ShellRunner, J: RunJournal>(
    pipeline: &Pipeline,
    agents: &[AgentDefinition],
    agent_runner: &mut A,
    shell_runner: &mut S,
    completed_steps: &[StepResult],
    journal: &mut J,
    max_step_executions: usize,
) -> Result<RunReport, RunError> {
    let Some(first_step) = pipeline.steps.first() else {
        return Err(RunError::EmptyPipeline);
    };
    let step_index: HashMap<&str, &Step> = pipeline
        .steps
        .iter()
        .map(|step| (step.id.as_str(), step))
        .collect();
    let agent_index: HashMap<&str, &AgentDefinition> = agents
        .iter()
        .map(|agent| (agent.name.as_str(), agent))
        .collect();
    let mut results = Vec::with_capacity(completed_steps.len());
    let mut current_id = first_step.id.clone();

    for (index, saved) in completed_steps.iter().enumerate() {
        if saved.step_id != current_id {
            return Err(RunError::InvalidResumeState {
                message: format!(
                    "saved step `{}` does not follow expected step `{current_id}`",
                    saved.step_id
                ),
            });
        }
        let step = step_index[current_id.as_str()];
        let mut matching = step
            .transitions
            .iter()
            .filter(|t| t.outcome == saved.outcome);
        let Some(transition) = matching.next() else {
            if index + 1 != completed_steps.len() {
                return Err(RunError::InvalidResumeState {
                    message: format!(
                        "saved terminal outcome `{}` from step `{}` is followed by more results",
                        saved.outcome, saved.step_id
                    ),
                });
            }
            let status = match saved.outcome.as_str() {
                "success" => RunStatus::Succeeded,
                "failure" => RunStatus::Failed,
                _ => {
                    return Err(RunError::UnknownOutcome {
                        step_id: step.id.clone(),
                        outcome: saved.outcome.clone(),
                    });
                }
            };
            results.push(saved.clone());
            return Ok(RunReport {
                pipeline_name: pipeline.name.clone(),
                status,
                steps: results,
            });
        };
        if matching.next().is_some() {
            return Err(RunError::AmbiguousOutcome {
                step_id: step.id.clone(),
                outcome: saved.outcome.clone(),
            });
        }
        if !step_index.contains_key(transition.target_step.as_str()) {
            return Err(RunError::MissingTarget {
                step_id: step.id.clone(),
                target: transition.target_step.clone(),
            });
        }
        current_id = transition.target_step.clone();
        results.push(saved.clone());
    }

    loop {
        if results.len() >= max_step_executions {
            return Err(RunError::StepLimitExceeded {
                limit: max_step_executions,
            });
        }
        let step = step_index
            .get(current_id.as_str())
            .copied()
            .ok_or_else(|| RunError::MissingTarget {
                step_id: results
                    .last()
                    .map_or_else(String::new, |r: &StepResult| r.step_id.clone()),
                target: current_id.clone(),
            })?;
        journal
            .step_started(&step.id)
            .map_err(|message| RunError::JournalFailed { message })?;

        let execution = match &step.kind {
            StepKind::Agent { agent } => {
                let definition = agent_index.get(agent.as_str()).copied().ok_or_else(|| {
                    RunError::MissingAgent {
                        step_id: step.id.clone(),
                        agent: agent.clone(),
                    }
                })?;
                agent_runner.run(definition, step, results.last())
            }
            StepKind::Shell { .. } => shell_runner.run(step),
        };
        let output = match execution {
            Ok(output) => output,
            Err(message) => {
                journal
                    .step_failed(&step.id, &message)
                    .map_err(|journal_error| RunError::JournalFailed {
                        message: journal_error,
                    })?;
                return Err(RunError::RunnerFailed {
                    step_id: step.id.clone(),
                    message,
                });
            }
        };

        let result = StepResult {
            step_id: step.id.clone(),
            outcome: output.outcome,
            output: output.output,
            exit_code: output.exit_code,
            runner_attempts: output.attempts,
            handoff: output.handoff,
        };
        journal
            .step_completed(&result)
            .map_err(|message| RunError::JournalFailed { message })?;
        let outcome = result.outcome.clone();
        results.push(result);

        let mut matching = step.transitions.iter().filter(|t| t.outcome == outcome);
        if let Some(transition) = matching.next() {
            if matching.next().is_some() {
                return Err(RunError::AmbiguousOutcome {
                    step_id: step.id.clone(),
                    outcome,
                });
            }
            if !step_index.contains_key(transition.target_step.as_str()) {
                return Err(RunError::MissingTarget {
                    step_id: step.id.clone(),
                    target: transition.target_step.clone(),
                });
            }
            current_id = transition.target_step.clone();
            continue;
        }

        return match outcome.as_str() {
            "success" => Ok(RunReport {
                pipeline_name: pipeline.name.clone(),
                status: RunStatus::Succeeded,
                steps: results,
            }),
            "failure" => Ok(RunReport {
                pipeline_name: pipeline.name.clone(),
                status: RunStatus::Failed,
                steps: results,
            }),
            _ => Err(RunError::UnknownOutcome {
                step_id: step.id.clone(),
                outcome,
            }),
        };
    }
}

struct NoopJournal;

impl RunJournal for NoopJournal {
    fn step_started(&mut self, _: &str) -> Result<(), String> {
        Ok(())
    }

    fn step_completed(&mut self, _: &StepResult) -> Result<(), String> {
        Ok(())
    }

    fn step_failed(&mut self, _: &str, _: &str) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::ports::{RunJournal, StepOutput};
    use super::*;
    use crate::{ModelRef, StepKind, Transition};
    use std::collections::VecDeque;

    #[derive(Default)]
    struct RecordingJournal {
        events: Vec<String>,
    }

    impl RunJournal for RecordingJournal {
        fn step_started(&mut self, step_id: &str) -> Result<(), String> {
            self.events.push(format!("started:{step_id}"));
            Ok(())
        }

        fn step_completed(&mut self, result: &StepResult) -> Result<(), String> {
            self.events.push(format!("completed:{}", result.step_id));
            Ok(())
        }

        fn step_failed(&mut self, step_id: &str, _: &str) -> Result<(), String> {
            self.events.push(format!("failed:{step_id}"));
            Ok(())
        }
    }

    struct FakeRunner {
        outputs: VecDeque<Result<StepOutput, String>>,
        previous_results: Vec<Option<StepResult>>,
    }

    impl FakeRunner {
        fn with(outcomes: &[&str]) -> Self {
            Self {
                outputs: outcomes
                    .iter()
                    .map(|outcome| {
                        Ok(StepOutput {
                            outcome: (*outcome).into(),
                            output: format!("output-{outcome}"),
                            exit_code: None,
                            attempts: 1,
                            handoff: None,
                        })
                    })
                    .collect(),
                previous_results: Vec::new(),
            }
        }

        fn failure(message: &str) -> Self {
            Self {
                outputs: VecDeque::from([Err(message.into())]),
                previous_results: Vec::new(),
            }
        }

        fn next(&mut self) -> Result<StepOutput, String> {
            self.outputs
                .pop_front()
                .unwrap_or_else(|| Err("no fake result".into()))
        }
    }

    impl AgentRunner for FakeRunner {
        fn run(
            &mut self,
            _: &AgentDefinition,
            _: &Step,
            previous_result: Option<&StepResult>,
        ) -> Result<StepOutput, String> {
            self.previous_results.push(previous_result.cloned());
            self.next()
        }
    }

    impl ShellRunner for FakeRunner {
        fn run(&mut self, _: &Step) -> Result<StepOutput, String> {
            self.next()
        }
    }

    fn agent() -> AgentDefinition {
        AgentDefinition {
            name: "coder".into(),
            harness: "test".into(),
            model: ModelRef::parse("test/model").unwrap(),
            thinking_level: None,
            prompt: "prompt".into(),
        }
    }

    fn agent_step(id: &str, transitions: &[(&str, &str)]) -> Step {
        Step {
            id: id.into(),
            kind: StepKind::Agent {
                agent: "coder".into(),
            },
            transitions: transitions
                .iter()
                .map(|(outcome, target_step)| Transition {
                    outcome: (*outcome).into(),
                    target_step: (*target_step).into(),
                })
                .collect(),
        }
    }

    fn shell_step(id: &str) -> Step {
        Step {
            id: id.into(),
            kind: StepKind::Shell {
                command: "test".into(),
                args: vec![],
            },
            transitions: vec![],
        }
    }

    #[test]
    fn runs_a_linear_pipeline_to_success() {
        let pipeline = Pipeline {
            name: "linear".into(),
            inputs: vec![],
            steps: vec![
                agent_step("build", &[("success", "test")]),
                shell_step("test"),
            ],
        };
        let mut agents = FakeRunner::with(&["success"]);
        let mut shells = FakeRunner::with(&["success"]);
        let report = run_pipeline(&pipeline, &[agent()], &mut agents, &mut shells, 10).unwrap();
        assert_eq!(report.status, RunStatus::Succeeded);
        assert_eq!(
            report
                .steps
                .iter()
                .map(|r| r.step_id.as_str())
                .collect::<Vec<_>>(),
            ["build", "test"]
        );
    }

    #[test]
    fn resumes_after_saved_steps_without_rerunning_them_and_journals_new_steps() {
        let pipeline = Pipeline {
            name: "resume".into(),
            inputs: vec![],
            steps: vec![
                agent_step("build", &[("success", "test")]),
                shell_step("test"),
            ],
        };
        let completed = [StepResult {
            step_id: "build".into(),
            outcome: "success".into(),
            output: "already built".into(),
            exit_code: Some(0),
            runner_attempts: 1,
            handoff: None,
        }];
        let mut agents = FakeRunner::with(&[]);
        let mut shells = FakeRunner::with(&["success"]);
        let mut journal = RecordingJournal::default();

        let report = run_pipeline_with_journal(
            &pipeline,
            &[agent()],
            &mut agents,
            &mut shells,
            &completed,
            &mut journal,
            10,
        )
        .unwrap();

        assert_eq!(report.status, RunStatus::Succeeded);
        assert_eq!(report.steps.len(), 2);
        assert!(agents.previous_results.is_empty(), "saved build was rerun");
        assert_eq!(journal.events, ["started:test", "completed:test"]);
    }

    #[test]
    fn supports_review_return_to_build() {
        let pipeline = Pipeline {
            name: "review".into(),
            inputs: vec![],
            steps: vec![
                agent_step("build", &[("success", "review")]),
                agent_step(
                    "review",
                    &[("changes_requested", "build"), ("approved", "finish")],
                ),
                shell_step("finish"),
            ],
        };
        let mut agents = FakeRunner::with(&["success", "changes_requested", "success", "approved"]);
        let mut shells = FakeRunner::with(&["success"]);
        let report = run_pipeline(&pipeline, &[agent()], &mut agents, &mut shells, 10).unwrap();
        assert_eq!(
            report
                .steps
                .iter()
                .map(|r| r.step_id.as_str())
                .collect::<Vec<_>>(),
            ["build", "review", "build", "review", "finish"]
        );
        let reviewer_feedback = agents.previous_results[2].as_ref().unwrap();
        assert_eq!(reviewer_feedback.step_id, "review");
        assert_eq!(reviewer_feedback.outcome, "changes_requested");
        assert_eq!(reviewer_feedback.output, "output-changes_requested");
    }

    #[test]
    fn returns_failed_terminal_status() {
        let pipeline = Pipeline {
            name: "failed".into(),
            inputs: vec![],
            steps: vec![shell_step("test")],
        };
        let mut agents = FakeRunner::with(&[]);
        let mut shells = FakeRunner::with(&["failure"]);
        let report = run_pipeline(&pipeline, &[], &mut agents, &mut shells, 1).unwrap();
        assert_eq!(report.status, RunStatus::Failed);
    }

    #[test]
    fn reports_runner_errors_and_unknown_outcomes() {
        let pipeline = Pipeline {
            name: "error".into(),
            inputs: vec![],
            steps: vec![shell_step("test")],
        };
        let mut agents = FakeRunner::with(&[]);
        let mut shells = FakeRunner::failure("exit code 1");
        assert_eq!(
            run_pipeline(&pipeline, &[], &mut agents, &mut shells, 1),
            Err(RunError::RunnerFailed {
                step_id: "test".into(),
                message: "exit code 1".into()
            })
        );

        let mut shells = FakeRunner::with(&["unexpected"]);
        assert_eq!(
            run_pipeline(&pipeline, &[], &mut agents, &mut shells, 1),
            Err(RunError::UnknownOutcome {
                step_id: "test".into(),
                outcome: "unexpected".into()
            })
        );
    }

    #[test]
    fn prevents_unbounded_transition_loops() {
        let pipeline = Pipeline {
            name: "loop".into(),
            inputs: vec![],
            steps: vec![agent_step("again", &[("again", "again")])],
        };
        let mut agents = FakeRunner::with(&["again", "again"]);
        let mut shells = FakeRunner::with(&[]);
        assert_eq!(
            run_pipeline(&pipeline, &[agent()], &mut agents, &mut shells, 2),
            Err(RunError::StepLimitExceeded { limit: 2 })
        );
    }
}
