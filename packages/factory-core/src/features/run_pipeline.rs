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
    RepeatedReviewFeedback,
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
            Self::RepeatedReviewFeedback => {
                write!(f, "run stopped after repeated identical reviewer feedback")
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
            StepKind::Agent { agent, .. } => {
                let definition = agent_index.get(agent.as_str()).copied().ok_or_else(|| {
                    RunError::MissingAgent {
                        step_id: step.id.clone(),
                        agent: agent.clone(),
                    }
                })?;
                agent_runner.run(definition, step, results.last())
            }
            StepKind::Shell { .. } => shell_runner.run(step, results.last()),
            StepKind::Human {
                prompt,
                options,
                approval_keyword,
            } => {
                let answer =
                    journal
                        .human_answer(&step.id, prompt, options)
                        .map_err(|message| RunError::RunnerFailed {
                            step_id: step.id.clone(),
                            message,
                        })?;
                let option_id = if options.is_empty() {
                    None
                } else {
                    let Some(option) = options.iter().find(|option| option.id == answer) else {
                        return Err(RunError::RunnerFailed {
                            step_id: step.id.clone(),
                            message: format!(
                                "answer must match one of the option IDs: {}",
                                options
                                    .iter()
                                    .map(|option| option.id.as_str())
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            ),
                        });
                    };
                    Some(option.id.clone())
                };
                if answer.trim().is_empty() {
                    return Err(RunError::RunnerFailed {
                        step_id: step.id.clone(),
                        message: "human answer must not be empty".into(),
                    });
                }
                let outcome = match approval_keyword {
                    Some(keyword) if answer == *keyword => "approved",
                    Some(_) => "feedback",
                    None => "success",
                };
                Ok(super::ports::StepOutput {
                    outcome: outcome.into(),
                    output: answer.clone(),
                    exit_code: None,
                    attempts: 1,
                    handoff: Some(super::run::StepHandoff::Human {
                        prompt: prompt.clone(),
                        answer,
                        option_id,
                    }),
                })
            }
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
        if outcome == "changes_requested"
            && result.handoff.is_some()
            && results.iter().any(|previous: &StepResult| {
                previous.outcome == "changes_requested"
                    && same_review_feedback(previous.handoff.as_ref(), result.handoff.as_ref())
            })
        {
            let proceed = journal
                .confirm_repeated_review_feedback(&result)
                .map_err(|message| RunError::JournalFailed { message })?;
            if !proceed {
                return Err(RunError::RepeatedReviewFeedback);
            }
        }
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

fn same_review_feedback(
    previous: Option<&super::run::StepHandoff>,
    current: Option<&super::run::StepHandoff>,
) -> bool {
    use super::run::StepHandoff;

    let (Some(StepHandoff::Review(previous)), Some(StepHandoff::Review(current))) =
        (previous, current)
    else {
        return false;
    };

    fn signature(handoff: &super::run::ReviewHandoff) -> Vec<(&str, &str, &str, &str, &str)> {
        let mut findings: Vec<_> = handoff
            .findings
            .iter()
            .map(|finding| {
                (
                    finding.title.as_str(),
                    finding.location.as_str(),
                    finding.problem.as_str(),
                    finding.required_change.as_str(),
                    finding.verification.as_str(),
                )
            })
            .collect();
        findings.sort_unstable();
        findings
    }

    signature(previous) == signature(current)
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
    use crate::features::{ReviewFinding, ReviewHandoff, StepHandoff};
    use crate::{ModelRef, StepKind, Transition};
    use std::collections::VecDeque;

    #[derive(Default)]
    struct RecordingJournal {
        events: Vec<String>,
        confirmations: usize,
        proceed_on_repeat: bool,
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

        fn confirm_repeated_review_feedback(&mut self, _: &StepResult) -> Result<bool, String> {
            self.confirmations += 1;
            Ok(self.proceed_on_repeat)
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
        fn run(
            &mut self,
            _: &Step,
            previous_result: Option<&StepResult>,
        ) -> Result<StepOutput, String> {
            self.previous_results.push(previous_result.cloned());
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
                context: vec![],
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

    fn finding(title: &str) -> ReviewFinding {
        ReviewFinding {
            title: title.into(),
            location: format!("src/{title}.rs"),
            problem: format!("problem {title}"),
            required_change: format!("change {title}"),
            verification: format!("verify {title}"),
        }
    }

    fn review_output(outcome: &str, findings: &[ReviewFinding]) -> StepOutput {
        StepOutput {
            outcome: outcome.into(),
            output: outcome.into(),
            exit_code: None,
            attempts: 1,
            handoff: Some(StepHandoff::Review(ReviewHandoff {
                findings: findings.to_vec(),
            })),
        }
    }

    fn review_loop_pipeline() -> Pipeline {
        Pipeline {
            name: "review-loop".into(),
            inputs: vec![],
            steps: vec![
                agent_step("build", &[("success", "review")]),
                agent_step(
                    "review",
                    &[("changes_requested", "build"), ("approved", "finish")],
                ),
                shell_step("finish"),
            ],
        }
    }

    #[test]
    fn reordered_identical_findings_escalate_and_aborting_preserves_review_history() {
        let first = finding("one");
        let second = finding("two");
        let mut agents = FakeRunner {
            outputs: VecDeque::from([
                Ok(StepOutput {
                    outcome: "success".into(),
                    output: String::new(),
                    exit_code: None,
                    attempts: 1,
                    handoff: None,
                }),
                Ok(review_output(
                    "changes_requested",
                    &[first.clone(), second.clone()],
                )),
                Ok(StepOutput {
                    outcome: "success".into(),
                    output: String::new(),
                    exit_code: None,
                    attempts: 1,
                    handoff: None,
                }),
                Ok(review_output("changes_requested", &[second, first])),
            ]),
            previous_results: Vec::new(),
        };
        let mut shells = FakeRunner::with(&[]);
        let mut journal = RecordingJournal::default();

        let result = run_pipeline_with_journal(
            &review_loop_pipeline(),
            &[agent()],
            &mut agents,
            &mut shells,
            &[],
            &mut journal,
            10,
        );

        assert_eq!(result, Err(RunError::RepeatedReviewFeedback));
        assert_eq!(journal.confirmations, 1);
        assert_eq!(
            journal
                .events
                .iter()
                .filter(|event| event.starts_with("completed:review"))
                .count(),
            2
        );
    }

    #[test]
    fn different_findings_do_not_escalate_but_repeated_feedback_can_continue() {
        let mut agents = FakeRunner {
            outputs: VecDeque::from([
                Ok(StepOutput {
                    outcome: "success".into(),
                    output: String::new(),
                    exit_code: None,
                    attempts: 1,
                    handoff: None,
                }),
                Ok(review_output("changes_requested", &[finding("one")])),
                Ok(StepOutput {
                    outcome: "success".into(),
                    output: String::new(),
                    exit_code: None,
                    attempts: 1,
                    handoff: None,
                }),
                Ok(review_output("changes_requested", &[finding("different")])),
                Ok(StepOutput {
                    outcome: "success".into(),
                    output: String::new(),
                    exit_code: None,
                    attempts: 1,
                    handoff: None,
                }),
                Ok(review_output("changes_requested", &[finding("different")])),
                Ok(StepOutput {
                    outcome: "success".into(),
                    output: String::new(),
                    exit_code: None,
                    attempts: 1,
                    handoff: None,
                }),
                Ok(review_output("approved", &[])),
            ]),
            previous_results: Vec::new(),
        };
        let mut shells = FakeRunner::with(&["success"]);
        let mut journal = RecordingJournal {
            proceed_on_repeat: true,
            ..RecordingJournal::default()
        };

        let report = run_pipeline_with_journal(
            &review_loop_pipeline(),
            &[agent()],
            &mut agents,
            &mut shells,
            &[],
            &mut journal,
            12,
        )
        .unwrap();

        assert_eq!(report.status, RunStatus::Succeeded);
        assert_eq!(
            journal.confirmations, 1,
            "only the exact repeated finding should escalate"
        );
        assert!(report.steps.iter().any(|step| step.step_id == "finish"));
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
