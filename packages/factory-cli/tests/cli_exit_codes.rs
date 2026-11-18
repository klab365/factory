use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("factory-cli-test-{unique}"));
        fs::create_dir_all(root.join(".factory/pipelines")).unwrap();
        fs::create_dir_all(root.join(".factory/agents")).unwrap();
        Self { root }
    }

    fn pipeline(&self, contents: &str) {
        fs::write(self.root.join(".factory/pipelines/test.toml"), contents).unwrap();
    }

    fn run(&self, pipeline: &str) -> Output {
        self.command(pipeline).output().unwrap()
    }

    fn command(&self, pipeline: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_factory"));
        command
            .args([pipeline, "--input", "task=test"])
            .current_dir(&self.root);
        command
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn write_fake_pi(root: &Path, outcome: &str) -> PathBuf {
    let path = root.join("fake-pi");
    let response = format!(
        r#"{{"type":"message_end","message":{{"role":"assistant","content":[{{"type":"text","text":"{{\"outcome\":\"{outcome}\",\"output\":\"controlled fixture\"}}"}}]}}}}"#
    );
    fs::write(
        &path,
        format!("#!/bin/sh\ncat <<'EVENT'\n{response}\nEVENT\n"),
    )
    .unwrap();
    let mut permissions = fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).unwrap();
    path
}

fn exit_code(output: Output) -> i32 {
    output.status.code().unwrap()
}

#[test]
fn validate_and_plan_do_not_execute_steps_or_create_run_artifacts() {
    let fixture = Fixture::new();
    fixture.pipeline("name = 'test'\n[[inputs]]\nkey = 'task'\ndescription = 'Task'\n[[steps]]\nid = 'agent-step'\ntype = 'agent'\nagent = 'worker'\n[[steps.transitions]]\noutcome = 'success'\ntarget_step = 'shell-step'\n[[steps]]\nid = 'shell-step'\ntype = 'shell'\ncommand = '/bin/touch'\nargs = ['marker']\n");
    fs::write(fixture.root.join(".factory/agents/worker.toml"), "name = 'worker'\nharness = 'pi'\nmodel = 'test/model'\nthinking_level = 'off'\nprompt = 'test'\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_factory"))
        .args(["plan", "test", "--input", "task=example"])
        .current_dir(&fixture.root)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("agent-step (agent)"));
    assert!(stdout.contains("shell-step (shell)"));
    assert!(stdout.contains("success` -> `shell-step"));
    assert!(!fixture.root.join("marker").exists());
    let runs = fixture.root.join(".factory/runs");
    assert!(!runs.exists());
    let validation = Command::new(env!("CARGO_BIN_EXE_factory"))
        .arg("validate")
        .current_dir(&fixture.root)
        .output()
        .unwrap();
    assert_eq!(
        validation.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&validation.stderr)
    );
}

#[test]
fn validate_reports_configuration_file_and_actionable_error() {
    let fixture = Fixture::new();
    fixture.pipeline("name = 'test'\n[[inputs]]\nkey = 'task'\ndescription = 'Task'\n[[steps]]\nid = 'agent'\ntype = 'agent'\nagent = 'missing'\n");
    let path = fixture.root.join(".factory/pipelines/test.toml");
    let output = Command::new(env!("CARGO_BIN_EXE_factory"))
        .arg("validate")
        .current_dir(&fixture.root)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(path.to_str().unwrap()));
    assert!(stderr.contains("unknown agent"));
    assert!(stderr.contains("step `agent`"));
}

#[test]
fn validate_rejects_unavailable_shell_outcomes_and_accepts_success_and_failure() {
    let fixture = Fixture::new();
    fixture.pipeline("name = 'test'\n[[inputs]]\nkey = 'task'\ndescription = 'Task'\n[[steps]]\nid = 'shell'\ntype = 'shell'\ncommand = 'true'\n[[steps.transitions]]\noutcome = 'typo'\ntarget_step = 'next'\n[[steps]]\nid = 'next'\ntype = 'shell'\ncommand = 'true'\n");

    let invalid = Command::new(env!("CARGO_BIN_EXE_factory"))
        .arg("validate")
        .current_dir(&fixture.root)
        .output()
        .unwrap();
    assert_eq!(invalid.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&invalid.stderr);
    assert!(stderr.contains("shell step `shell`"));
    assert!(stderr.contains("unsupported transition outcome `typo`"));
    assert!(stderr.contains("`success` or `failure`"));

    fixture.pipeline("name = 'test'\n[[inputs]]\nkey = 'task'\ndescription = 'Task'\n[[steps]]\nid = 'shell'\ntype = 'shell'\ncommand = 'true'\n[[steps.transitions]]\noutcome = 'success'\ntarget_step = 'success-step'\n[[steps.transitions]]\noutcome = 'failure'\ntarget_step = 'failure-step'\n[[steps]]\nid = 'success-step'\ntype = 'shell'\ncommand = 'true'\n[[steps]]\nid = 'failure-step'\ntype = 'shell'\ncommand = 'true'\n");
    let valid = Command::new(env!("CARGO_BIN_EXE_factory"))
        .arg("validate")
        .current_dir(&fixture.root)
        .output()
        .unwrap();
    assert_eq!(
        valid.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&valid.stderr)
    );
}

#[test]
fn plan_reports_unknown_pipeline_and_missing_inputs() {
    let fixture = Fixture::new();
    fixture.pipeline("name = 'test'\n[[inputs]]\nkey = 'task'\ndescription = 'Task'\n[[steps]]\nid = 'shell'\ntype = 'shell'\ncommand = 'true'\n");
    let command = |pipeline: &str| {
        Command::new(env!("CARGO_BIN_EXE_factory"))
            .args(["plan", pipeline])
            .current_dir(&fixture.root)
            .output()
            .unwrap()
    };
    let missing_pipeline = command("unknown");
    assert_eq!(missing_pipeline.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&missing_pipeline.stderr).contains("not found"));
    let missing_input = command("test");
    assert_eq!(missing_input.status.code(), Some(64));
    assert!(String::from_utf8_lossy(&missing_input.stderr).contains("required input `task`"));
}

#[test]
fn binary_returns_three_for_a_failed_shell_command() {
    let fixture = Fixture::new();
    fixture.pipeline(
        "name = 'test'\n[[inputs]]\nkey = 'task'\ndescription = 'Task'\n[[steps]]\nid = 'shell'\ntype = 'shell'\ncommand = '/bin/false'\n",
    );
    assert_eq!(exit_code(fixture.run("test")), 3);
}

#[test]
fn binary_returns_four_for_terminal_agent_failure() {
    let fixture = Fixture::new();
    let pi = write_fake_pi(&fixture.root, "failure");
    fixture.pipeline(
        "name = 'test'\n[[inputs]]\nkey = 'task'\ndescription = 'Task'\n[[steps]]\nid = 'agent-step'\ntype = 'agent'\nagent = 'worker'\n",
    );
    fs::write(
        fixture.root.join(".factory/agents/worker.toml"),
        "name = 'worker'\nharness = 'pi'\nmodel = 'test/model'\nthinking_level = 'off'\nprompt = 'test'\n",
    )
    .unwrap();
    let result = fixture.command("test").env("PI_CLI", pi).output().unwrap();
    assert_eq!(
        result.status.code().unwrap(),
        4,
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn run_show_returns_complete_json_with_results_and_retry_history() {
    let fixture = Fixture::new();
    fixture.pipeline("name = 'test'\n[[inputs]]\nkey = 'task'\ndescription = 'Task'\n[[steps]]\nid = 'shell'\ntype = 'shell'\ncommand = '/bin/sh'\nargs = ['-c', 'if [ -e marker ]; then echo retried; else touch marker; echo first-attempt; exit 1; fi']\n");
    let initial = fixture.run("test");
    assert_eq!(initial.status.code(), Some(3));
    let run_id = String::from_utf8_lossy(&initial.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("Run ID: "))
        .unwrap()
        .to_owned();
    let record_path = fixture
        .root
        .join(".factory/runs")
        .join(&run_id)
        .join("run.json");
    let mut record: serde_json::Value =
        serde_json::from_slice(&fs::read(&record_path).unwrap()).unwrap();
    record["status"] = serde_json::json!("interrupted");
    record["steps"][0]["status"] = serde_json::json!("interrupted");
    fs::write(&record_path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
    let retry = Command::new(env!("CARGO_BIN_EXE_factory"))
        .args(["run", "resume", &run_id, "--retry-current"])
        .current_dir(&fixture.root)
        .output()
        .unwrap();
    assert_eq!(
        retry.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    let shown = Command::new(env!("CARGO_BIN_EXE_factory"))
        .args(["run", "show", &run_id])
        .current_dir(&fixture.root)
        .output()
        .unwrap();
    assert_eq!(shown.status.code(), Some(0));
    let json: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
    assert_eq!(json["steps"].as_array().unwrap().len(), 2);
    assert_eq!(json["steps"][0]["result"]["output"], "first-attempt\n");
    assert_eq!(json["steps"][0]["result"]["exit_code"], 1);
    assert_eq!(json["steps"][1]["attempt"], 2);
    assert_eq!(json["steps"][1]["result"]["output"], "retried\n");
    assert_eq!(json["steps"][1]["result"]["runner_attempts"], 1);
}

#[test]
fn binary_returns_two_for_configuration_errors() {
    let fixture = Fixture::new();
    assert_eq!(exit_code(fixture.run("missing")), 2);
}

#[test]
fn binary_returns_three_for_runner_errors() {
    let fixture = Fixture::new();
    fixture.pipeline(
        "name = 'test'\n[[inputs]]\nkey = 'task'\ndescription = 'Task'\n[[steps]]\nid = 'shell'\ntype = 'shell'\ncommand = '/no/such/runner'\n",
    );
    assert_eq!(exit_code(fixture.run("test")), 3);
}
