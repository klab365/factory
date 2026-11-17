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
