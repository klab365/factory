//! Durable JSON persistence for pipeline runs.

use std::{
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use factory_core::features::{ReviewFinding, ReviewHandoff, RunJournal, StepHandoff, StepResult};
use serde::{Deserialize, Serialize};

const RUN_RECORD_VERSION: u32 = 1;

#[derive(Debug)]
pub struct RunStoreError {
    pub path: PathBuf,
    pub message: String,
}

impl fmt::Display for RunStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path.display(), self.message)
    }
}

impl Error for RunStoreError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedRunStatus {
    Running,
    Interrupted,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepAttemptStatus {
    Running,
    Completed,
    Error,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum PersistedHandoff {
    Review {
        findings: Vec<PersistedReviewFinding>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedReviewFinding {
    pub title: String,
    pub location: String,
    pub problem: String,
    pub required_change: String,
    pub verification: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedStepResult {
    pub step_id: String,
    pub outcome: String,
    pub output: String,
    pub exit_code: Option<i32>,
    pub runner_attempts: u32,
    pub handoff: Option<PersistedHandoff>,
}

impl From<&StepResult> for PersistedStepResult {
    fn from(result: &StepResult) -> Self {
        Self {
            step_id: result.step_id.clone(),
            outcome: result.outcome.clone(),
            output: result.output.clone(),
            exit_code: result.exit_code,
            runner_attempts: result.runner_attempts,
            handoff: result.handoff.as_ref().map(PersistedHandoff::from),
        }
    }
}

impl From<&PersistedStepResult> for StepResult {
    fn from(result: &PersistedStepResult) -> Self {
        Self {
            step_id: result.step_id.clone(),
            outcome: result.outcome.clone(),
            output: result.output.clone(),
            exit_code: result.exit_code,
            runner_attempts: result.runner_attempts,
            handoff: result.handoff.as_ref().map(StepHandoff::from),
        }
    }
}

impl From<&StepHandoff> for PersistedHandoff {
    fn from(handoff: &StepHandoff) -> Self {
        match handoff {
            StepHandoff::Review(review) => Self::Review {
                findings: review
                    .findings
                    .iter()
                    .map(|finding| PersistedReviewFinding {
                        title: finding.title.clone(),
                        location: finding.location.clone(),
                        problem: finding.problem.clone(),
                        required_change: finding.required_change.clone(),
                        verification: finding.verification.clone(),
                    })
                    .collect(),
            },
        }
    }
}

impl From<&PersistedHandoff> for StepHandoff {
    fn from(handoff: &PersistedHandoff) -> Self {
        match handoff {
            PersistedHandoff::Review { findings } => Self::Review(ReviewHandoff {
                findings: findings
                    .iter()
                    .map(|finding| ReviewFinding {
                        title: finding.title.clone(),
                        location: finding.location.clone(),
                        problem: finding.problem.clone(),
                        required_change: finding.required_change.clone(),
                        verification: finding.verification.clone(),
                    })
                    .collect(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepAttemptRecord {
    pub step_id: String,
    pub attempt: u32,
    pub status: StepAttemptStatus,
    pub started_at_ms: u128,
    pub finished_at_ms: Option<u128>,
    pub result: Option<PersistedStepResult>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    pub version: u32,
    pub run_id: String,
    pub pipeline_name: String,
    pub task: String,
    #[serde(default)]
    pub inputs: std::collections::HashMap<String, String>,
    pub config_snapshot: String,
    pub status: PersistedRunStatus,
    pub current_step: Option<String>,
    pub started_at_ms: u128,
    pub updated_at_ms: u128,
    pub finished_at_ms: Option<u128>,
    pub steps: Vec<StepAttemptRecord>,
}

pub struct RunStore {
    run_dir: PathBuf,
    record: RunRecord,
}

impl RunStore {
    pub fn create(
        run_dir: impl Into<PathBuf>,
        run_id: String,
        pipeline_name: String,
        task: String,
        inputs: std::collections::HashMap<String, String>,
    ) -> Result<Self, RunStoreError> {
        let run_dir = run_dir.into();
        let now = now_ms()?;
        let mut store = Self {
            run_dir,
            record: RunRecord {
                version: RUN_RECORD_VERSION,
                run_id,
                pipeline_name,
                task,
                inputs,
                config_snapshot: "config.json".into(),
                status: PersistedRunStatus::Running,
                current_step: None,
                started_at_ms: now,
                updated_at_ms: now,
                finished_at_ms: None,
                steps: Vec::new(),
            },
        };
        store.save()?;
        Ok(store)
    }

    pub fn open(run_dir: impl Into<PathBuf>) -> Result<Self, RunStoreError> {
        let run_dir = run_dir.into();
        let path = run_dir.join("run.json");
        let bytes = fs::read(&path).map_err(|error| store_error(&path, error.to_string()))?;
        let record: RunRecord = serde_json::from_slice(&bytes)
            .map_err(|error| store_error(&path, format!("invalid run record: {error}")))?;
        if record.version != RUN_RECORD_VERSION {
            return Err(store_error(
                &path,
                format!("unsupported run record version `{}`", record.version),
            ));
        }
        Ok(Self { run_dir, record })
    }

    pub fn record(&self) -> &RunRecord {
        &self.record
    }

    pub fn run_dir(&self) -> &Path {
        &self.run_dir
    }

    pub fn completed_results(&self) -> Vec<StepResult> {
        self.record
            .steps
            .iter()
            .filter(|step| step.status == StepAttemptStatus::Completed)
            .filter_map(|step| step.result.as_ref())
            .map(StepResult::from)
            .collect()
    }

    pub fn latest_attempt_status(&self) -> Option<StepAttemptStatus> {
        self.record.steps.last().map(|step| step.status)
    }

    pub fn has_unresolved_attempt(&self) -> bool {
        matches!(
            self.latest_attempt_status(),
            Some(
                StepAttemptStatus::Running
                    | StepAttemptStatus::Error
                    | StepAttemptStatus::Interrupted
            )
        )
    }

    pub fn mark_interrupted(&mut self) -> Result<(), RunStoreError> {
        self.record.status = PersistedRunStatus::Interrupted;
        for step in self.record.steps.iter_mut().rev() {
            if step.status == StepAttemptStatus::Running {
                step.status = StepAttemptStatus::Interrupted;
                step.finished_at_ms = Some(now_ms()?);
                break;
            }
        }
        self.save()
    }

    pub fn finish(&mut self, status: PersistedRunStatus) -> Result<(), RunStoreError> {
        self.record.status = status;
        let now = now_ms()?;
        self.record.updated_at_ms = now;
        self.record.finished_at_ms = Some(now);
        self.save()
    }

    pub fn display_json(&self) -> Result<String, RunStoreError> {
        serde_json::to_string_pretty(&self.record)
            .map_err(|error| store_error(self.run_dir.join("run.json"), error.to_string()))
    }

    fn save(&mut self) -> Result<(), RunStoreError> {
        self.record.updated_at_ms = now_ms()?;
        let path = self.run_dir.join("run.json");
        let temporary = self
            .run_dir
            .join(format!(".run-{}.tmp", std::process::id()));
        let bytes = serde_json::to_vec_pretty(&self.record)
            .map_err(|error| store_error(&path, format!("could not serialize run: {error}")))?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| store_error(&temporary, error.to_string()))?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| store_error(&temporary, error.to_string()))?;
        fs::rename(&temporary, &path).map_err(|error| {
            let _ = fs::remove_file(&temporary);
            store_error(
                &path,
                format!("could not atomically replace run record: {error}"),
            )
        })?;
        File::open(&self.run_dir)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| store_error(&self.run_dir, error.to_string()))?;
        Ok(())
    }
}

impl RunJournal for RunStore {
    fn step_started(&mut self, step_id: &str) -> Result<(), String> {
        let attempt = self
            .record
            .steps
            .iter()
            .filter(|step| step.step_id == step_id)
            .count() as u32
            + 1;
        let started_at_ms = now_ms().map_err(|error| error.to_string())?;
        self.record.current_step = Some(step_id.into());
        self.record.status = PersistedRunStatus::Running;
        self.record.finished_at_ms = None;
        self.record.steps.push(StepAttemptRecord {
            step_id: step_id.into(),
            attempt,
            status: StepAttemptStatus::Running,
            started_at_ms,
            finished_at_ms: None,
            result: None,
            error: None,
        });
        self.save().map_err(|error| error.to_string())
    }

    fn step_completed(&mut self, result: &StepResult) -> Result<(), String> {
        let Some(step) = self.record.steps.iter_mut().rev().find(|step| {
            step.step_id == result.step_id && step.status == StepAttemptStatus::Running
        }) else {
            return Err(format!("no running attempt for step `{}`", result.step_id));
        };
        step.status = StepAttemptStatus::Completed;
        step.finished_at_ms = Some(now_ms().map_err(|error| error.to_string())?);
        step.result = Some(PersistedStepResult::from(result));
        self.save().map_err(|error| error.to_string())
    }

    fn step_failed(&mut self, step_id: &str, error: &str) -> Result<(), String> {
        let Some(step) = self
            .record
            .steps
            .iter_mut()
            .rev()
            .find(|step| step.step_id == step_id && step.status == StepAttemptStatus::Running)
        else {
            return Err(format!("no running attempt for step `{step_id}`"));
        };
        step.status = StepAttemptStatus::Error;
        step.finished_at_ms = Some(now_ms().map_err(|error| error.to_string())?);
        step.error = Some(error.into());
        self.save().map_err(|error| error.to_string())
    }
}

fn now_ms() -> Result<u128, RunStoreError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .map_err(|error| store_error("system clock", error.to_string()))
}

fn store_error(path: impl Into<PathBuf>, message: String) -> RunStoreError {
    RunStoreError {
        path: path.into(),
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persists_step_execution_errors_for_run_inspection() {
        let run_dir =
            std::env::temp_dir().join(format!("factory-run-error-{}", std::process::id()));
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(&run_dir).unwrap();
        let mut store = RunStore::create(
            &run_dir,
            "run-error".into(),
            "feature".into(),
            "task".into(),
            std::collections::HashMap::new(),
        )
        .unwrap();
        store.step_started("build").unwrap();
        store
            .step_failed("build", "Pi returned HTTP 503 after 3 attempts")
            .unwrap();
        store.finish(PersistedRunStatus::Failed).unwrap();
        drop(store);

        let store = RunStore::open(&run_dir).unwrap();
        assert_eq!(store.record().status, PersistedRunStatus::Failed);
        assert!(store.has_unresolved_attempt());
        assert_eq!(store.record().steps[0].status, StepAttemptStatus::Error);
        assert!(
            store.record().steps[0]
                .error
                .as_deref()
                .unwrap()
                .contains("HTTP 503")
        );
        fs::remove_dir_all(run_dir).unwrap();
    }

    #[test]
    fn persists_step_lifecycle_and_reopens_completed_results() {
        let run_dir =
            std::env::temp_dir().join(format!("factory-run-store-{}", std::process::id()));
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(&run_dir).unwrap();
        let mut store = RunStore::create(
            &run_dir,
            "run-1".into(),
            "feature".into(),
            "task".into(),
            std::collections::HashMap::from([("target".into(), "value".into())]),
        )
        .unwrap();
        store.step_started("build").unwrap();
        let result = StepResult {
            step_id: "build".into(),
            outcome: "success".into(),
            output: "done".into(),
            exit_code: Some(0),
            runner_attempts: 2,
            handoff: Some(StepHandoff::Review(ReviewHandoff {
                findings: vec![ReviewFinding {
                    title: "Test gap".into(),
                    location: "src/lib.rs".into(),
                    problem: "A case is missing.".into(),
                    required_change: "Add a test.".into(),
                    verification: "The new test passes.".into(),
                }],
            })),
        };
        store.step_completed(&result).unwrap();
        drop(store);

        let store = RunStore::open(&run_dir).unwrap();
        assert_eq!(store.record().status, PersistedRunStatus::Running);
        assert_eq!(store.record().inputs["target"], "value");
        assert_eq!(store.completed_results(), [result]);
        assert_eq!(store.record().current_step.as_deref(), Some("build"));
        fs::remove_dir_all(run_dir).unwrap();
    }
}
