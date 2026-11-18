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
    Cancelled,
    WaitingForInput,
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
    Cancelled,
    WaitingForInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingHuman {
    pub step_id: String,
    pub prompt: String,
    pub options: Vec<PersistedHumanOption>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedHumanOption {
    pub id: String,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum PersistedHandoff {
    Review {
        findings: Vec<PersistedReviewFinding>,
    },
    Human {
        prompt: String,
        answer: String,
        option_id: Option<String>,
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
            StepHandoff::Human {
                prompt,
                answer,
                option_id,
            } => Self::Human {
                prompt: prompt.clone(),
                answer: answer.clone(),
                option_id: option_id.clone(),
            },
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
            PersistedHandoff::Human {
                prompt,
                answer,
                option_id,
            } => Self::Human {
                prompt: prompt.clone(),
                answer: answer.clone(),
                option_id: option_id.clone(),
            },
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
    #[serde(default)]
    pub pending_human: Option<PendingHuman>,
    pub started_at_ms: u128,
    pub updated_at_ms: u128,
    pub finished_at_ms: Option<u128>,
    #[serde(default)]
    pub cancelled_at_ms: Option<u128>,
    #[serde(default)]
    pub cancelled_step: Option<String>,
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
                pending_human: None,
                started_at_ms: now,
                updated_at_ms: now,
                finished_at_ms: None,
                cancelled_at_ms: None,
                cancelled_step: None,
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
        ) && self.record.status != PersistedRunStatus::WaitingForInput
    }

    pub fn wait_for_human(
        &mut self,
        step_id: &str,
        prompt: &str,
        options: &[factory_core::HumanOption],
    ) -> Result<(), RunStoreError> {
        let previous_record = self.record.clone();
        self.record.pending_human = Some(PendingHuman {
            step_id: step_id.into(),
            prompt: prompt.into(),
            options: options
                .iter()
                .map(|o| PersistedHumanOption {
                    id: o.id.clone(),
                    label: o.label.clone(),
                })
                .collect(),
        });
        self.record.status = PersistedRunStatus::WaitingForInput;
        if let Some(attempt) = self
            .record
            .steps
            .iter_mut()
            .rev()
            .find(|s| s.step_id == step_id && s.status == StepAttemptStatus::Running)
        {
            attempt.status = StepAttemptStatus::WaitingForInput;
        }
        if let Err(error) = self.save() {
            self.record = previous_record;
            return Err(error);
        }
        Ok(())
    }

    pub fn answer_pending_human(&mut self, answer: &str) -> Result<StepResult, String> {
        let pending = self
            .record
            .pending_human
            .clone()
            .ok_or("run has no pending human question")?;
        let answer = answer.trim();
        if answer.is_empty() {
            return Err("Human answer must not be empty".into());
        }
        let option_id = if pending.options.is_empty() {
            None
        } else {
            Some(
                pending
                    .options
                    .iter()
                    .find(|o| o.id == answer)
                    .ok_or_else(|| {
                        format!(
                            "answer must match one of: {}",
                            pending
                                .options
                                .iter()
                                .map(|o| o.id.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    })?
                    .id
                    .clone(),
            )
        };
        let result = StepResult {
            step_id: pending.step_id.clone(),
            outcome: "success".into(),
            output: answer.into(),
            exit_code: None,
            runner_attempts: 1,
            handoff: Some(StepHandoff::Human {
                prompt: pending.prompt,
                answer: answer.into(),
                option_id,
            }),
        };
        let attempt = self
            .record
            .steps
            .iter_mut()
            .rev()
            .find(|s| s.step_id == result.step_id && s.status == StepAttemptStatus::WaitingForInput)
            .ok_or("pending human step attempt is missing")?;
        attempt.status = StepAttemptStatus::Completed;
        attempt.finished_at_ms = Some(now_ms().map_err(|e| e.to_string())?);
        attempt.result = Some(PersistedStepResult::from(&result));
        self.record.pending_human = None;
        self.record.status = PersistedRunStatus::Running;
        self.save().map_err(|e| e.to_string())?;
        Ok(result)
    }

    pub fn cancel(&mut self) -> Result<(), RunStoreError> {
        let _lock = RunFileLock::acquire(&self.run_dir)?;
        self.record = self.load_latest()?;
        if matches!(
            self.record.status,
            PersistedRunStatus::Succeeded
                | PersistedRunStatus::Failed
                | PersistedRunStatus::Cancelled
        ) {
            return Err(store_error(
                self.run_dir.join("run.json"),
                "run is already terminal".into(),
            ));
        }
        let now = now_ms()?;
        self.record.status = PersistedRunStatus::Cancelled;
        self.record.cancelled_at_ms = Some(now);
        self.record.cancelled_step = self.record.current_step.clone();
        if let Some(active) = self.record.steps.iter_mut().rev().find(|step| {
            matches!(
                step.status,
                StepAttemptStatus::Running | StepAttemptStatus::WaitingForInput
            )
        }) {
            active.status = StepAttemptStatus::Cancelled;
            active.finished_at_ms = Some(now);
        }
        self.record.pending_human = None;
        self.record.finished_at_ms = Some(now);
        self.save_locked()
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
        let _lock = RunFileLock::acquire(&self.run_dir)?;
        self.reload_latest()?;
        self.save_locked()
    }

    fn load_latest(&self) -> Result<RunRecord, RunStoreError> {
        let path = self.run_dir.join("run.json");
        let bytes = fs::read(&path).map_err(|error| store_error(&path, error.to_string()))?;
        serde_json::from_slice(&bytes)
            .map_err(|error| store_error(&path, format!("invalid run record: {error}")))
    }

    fn reload_latest(&mut self) -> Result<(), RunStoreError> {
        let path = self.run_dir.join("run.json");
        let Ok(bytes) = fs::read(&path) else {
            return Ok(());
        };
        let latest: RunRecord = serde_json::from_slice(&bytes)
            .map_err(|error| store_error(&path, format!("invalid run record: {error}")))?;
        if latest.status == PersistedRunStatus::Cancelled
            && self.record.status != PersistedRunStatus::Cancelled
        {
            self.record = latest;
            return Err(store_error(&path, "run was cancelled".into()));
        }
        for newer in &latest.steps {
            if let Some(local) = self
                .record
                .steps
                .iter_mut()
                .find(|step| step.step_id == newer.step_id && step.attempt == newer.attempt)
            {
                if local.status == StepAttemptStatus::Running && local.finished_at_ms.is_none() {
                    *local = newer.clone();
                }
            } else {
                self.record.steps.push(newer.clone());
            }
        }
        self.record.steps.sort_by_key(|step| step.started_at_ms);
        if self.record.current_step.is_none() {
            self.record.current_step = latest.current_step;
        }
        Ok(())
    }

    fn save_locked(&mut self) -> Result<(), RunStoreError> {
        let path = self.run_dir.join("run.json");
        self.record.updated_at_ms = now_ms()?;
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
    fn is_cancelled(&self) -> bool {
        self.record.status == PersistedRunStatus::Cancelled
            || fs::read(self.run_dir.join("run.json"))
                .ok()
                .and_then(|bytes| serde_json::from_slice::<RunRecord>(&bytes).ok())
                .is_some_and(|record| record.status == PersistedRunStatus::Cancelled)
    }

    fn step_started(&mut self, step_id: &str) -> Result<(), String> {
        if self.is_cancelled() {
            return Err("run was cancelled".into());
        }
        let attempt = self
            .record
            .steps
            .iter()
            .filter(|step| step.step_id == step_id)
            .count() as u32
            + 1;
        let started_at_ms = now_ms().map_err(|error| error.to_string())?;
        if self.record.status == PersistedRunStatus::Cancelled {
            return Err("run was cancelled".into());
        }
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
            step.step_id == result.step_id
                && matches!(
                    step.status,
                    StepAttemptStatus::Running | StepAttemptStatus::WaitingForInput
                )
        }) else {
            return Err(format!("no running attempt for step `{}`", result.step_id));
        };
        step.status = StepAttemptStatus::Completed;
        step.finished_at_ms = Some(now_ms().map_err(|error| error.to_string())?);
        step.result = Some(PersistedStepResult::from(result));
        self.record.pending_human = None;
        self.record.status = PersistedRunStatus::Running;
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

struct RunFileLock(File);

impl RunFileLock {
    fn acquire(run_dir: &Path) -> Result<Self, RunStoreError> {
        let path = run_dir.join(".run.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| store_error(&path, error.to_string()))?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            unsafe extern "C" {
                fn flock(fd: i32, operation: i32) -> i32;
            }
            // LOCK_EX; the kernel releases this lock when the process exits.
            if unsafe { flock(file.as_raw_fd(), 2) } != 0 {
                return Err(store_error(
                    &path,
                    std::io::Error::last_os_error().to_string(),
                ));
            }
        }
        Ok(Self(file))
    }
}

impl Drop for RunFileLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            unsafe extern "C" {
                fn flock(fd: i32, operation: i32) -> i32;
            }
            // LOCK_UN
            let _ = unsafe { flock(self.0.as_raw_fd(), 8) };
        }
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
    fn persists_cancellation_and_rejects_terminal_transition() {
        let run_dir =
            std::env::temp_dir().join(format!("factory-run-cancel-{}", std::process::id()));
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(&run_dir).unwrap();
        let mut store = RunStore::create(
            &run_dir,
            "cancel-test".into(),
            "pipeline".into(),
            "task".into(),
            std::collections::HashMap::new(),
        )
        .unwrap();
        store.step_started("build").unwrap();
        store.cancel().unwrap();
        assert_eq!(store.record().cancelled_step.as_deref(), Some("build"));
        drop(store);
        let mut reopened = RunStore::open(&run_dir).unwrap();
        assert!(reopened.is_cancelled());
        assert!(reopened.record().cancelled_at_ms.is_some());
        assert!(reopened.cancel().is_err());
        fs::remove_dir_all(run_dir).unwrap();
    }

    #[test]
    fn cancel_closes_waiting_human_attempt_and_clears_pending_input() {
        let run_dir =
            std::env::temp_dir().join(format!("factory-run-cancel-human-{}", std::process::id()));
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(&run_dir).unwrap();
        let mut store = RunStore::create(
            &run_dir,
            "human-cancel".into(),
            "pipeline".into(),
            "task".into(),
            Default::default(),
        )
        .unwrap();
        store.step_started("ask").unwrap();
        store.wait_for_human("ask", "Question?", &[]).unwrap();

        store.cancel().unwrap();
        let persisted = RunStore::open(&run_dir).unwrap();
        assert_eq!(persisted.record().status, PersistedRunStatus::Cancelled);
        assert!(persisted.record().pending_human.is_none());
        assert_eq!(persisted.record().steps.len(), 1);
        assert_eq!(
            persisted.record().steps[0].status,
            StepAttemptStatus::Cancelled
        );
        assert!(persisted.record().steps[0].finished_at_ms.is_some());
        fs::remove_dir_all(run_dir).unwrap();
    }

    #[test]
    fn stale_cancel_store_cannot_replace_successful_terminal_status() {
        let run_dir =
            std::env::temp_dir().join(format!("factory-run-cancel-stale-{}", std::process::id()));
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(&run_dir).unwrap();
        let mut run = RunStore::create(
            &run_dir,
            "run".into(),
            "pipeline".into(),
            "task".into(),
            Default::default(),
        )
        .unwrap();
        run.step_started("build").unwrap();
        let mut stale_cancel = RunStore::open(&run_dir).unwrap();
        run.step_completed(&StepResult {
            step_id: "build".into(),
            outcome: "success".into(),
            output: String::new(),
            exit_code: Some(0),
            runner_attempts: 1,
            handoff: None,
        })
        .unwrap();
        run.finish(PersistedRunStatus::Succeeded).unwrap();
        assert!(stale_cancel.cancel().is_err());
        assert_eq!(
            RunStore::open(&run_dir).unwrap().record().status,
            PersistedRunStatus::Succeeded
        );
        fs::remove_dir_all(run_dir).unwrap();
    }

    #[test]
    fn cancel_uses_latest_step_after_step_change() {
        let run_dir =
            std::env::temp_dir().join(format!("factory-run-cancel-step-{}", std::process::id()));
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(&run_dir).unwrap();
        let mut run = RunStore::create(
            &run_dir,
            "run".into(),
            "pipeline".into(),
            "task".into(),
            Default::default(),
        )
        .unwrap();
        run.step_started("first").unwrap();
        let mut stale_cancel = RunStore::open(&run_dir).unwrap();
        run.step_failed("first", "retry").unwrap();
        run.step_started("second").unwrap();
        stale_cancel.cancel().unwrap();
        let record = RunStore::open(&run_dir).unwrap();
        assert_eq!(record.record().cancelled_step.as_deref(), Some("second"));
        fs::remove_dir_all(run_dir).unwrap();
    }

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
    fn stale_journal_cannot_overwrite_cancel_or_drop_persisted_attempts() {
        let run_dir = std::env::temp_dir().join(format!("factory-run-race-{}", std::process::id()));
        let _ = fs::remove_dir_all(&run_dir);
        fs::create_dir_all(&run_dir).unwrap();
        let mut journal = RunStore::create(
            &run_dir,
            "race-test".into(),
            "pipeline".into(),
            "task".into(),
            std::collections::HashMap::new(),
        )
        .unwrap();
        journal.step_started("build").unwrap();
        let mut cancel = RunStore::open(&run_dir).unwrap();
        cancel.cancel().unwrap();
        let result = StepResult {
            step_id: "build".into(),
            outcome: "success".into(),
            output: "done".into(),
            exit_code: Some(0),
            runner_attempts: 1,
            handoff: None,
        };
        assert!(journal.step_completed(&result).is_err());
        let persisted = RunStore::open(&run_dir).unwrap();
        assert_eq!(persisted.record().status, PersistedRunStatus::Cancelled);
        assert_eq!(persisted.record().steps.len(), 1);
        assert_eq!(
            persisted.record().steps[0].status,
            StepAttemptStatus::Cancelled
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
