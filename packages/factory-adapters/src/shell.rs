//! Explicit shell-command execution adapter with bounded runtime and environment.

use std::{
    collections::HashMap,
    ffi::OsString,
    io::Read,
    path::PathBuf,
    process::{Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use factory_core::{
    Step, StepKind,
    features::{ShellRunner, StepOutput},
};

#[derive(Debug, Clone)]
pub struct ShellRunnerConfig {
    pub working_directory: PathBuf,
    pub timeout: Duration,
    /// The only environment variables made available to child processes.
    pub environment: HashMap<OsString, OsString>,
    /// Pipeline inputs substituted in shell arguments using `{{name}}` placeholders.
    pub inputs: HashMap<String, String>,
}

impl ShellRunnerConfig {
    pub fn new(working_directory: impl Into<PathBuf>, timeout: Duration) -> Self {
        Self {
            working_directory: working_directory.into(),
            timeout,
            environment: HashMap::new(),
            inputs: HashMap::new(),
        }
    }
}

pub struct ShellRunnerAdapter {
    config: ShellRunnerConfig,
}

impl ShellRunnerAdapter {
    pub fn new(config: ShellRunnerConfig) -> Self {
        Self { config }
    }

    fn execute(&self, step: &Step, command: &str, args: &[String]) -> Result<StepOutput, String> {
        let resolved_args = args
            .iter()
            .map(|arg| substitute_inputs(arg, &self.config.inputs, &step.id))
            .collect::<Result<Vec<_>, _>>()?;
        let mut child = Command::new(command)
            .args(&resolved_args)
            .current_dir(&self.config.working_directory)
            .env_clear()
            .envs(&self.config.environment)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("could not start shell step `{}`: {error}", step.id))?;

        let stdout = child.stdout.take().expect("stdout was configured as piped");
        let stderr = child.stderr.take().expect("stderr was configured as piped");
        let stdout_reader = thread::spawn(move || read_all(stdout));
        let stderr_reader = thread::spawn(move || read_all(stderr));
        let started = Instant::now();

        let status = loop {
            match child
                .try_wait()
                .map_err(|error| format!("could not wait for shell step `{}`: {error}", step.id))?
            {
                Some(status) => break Some(status),
                None if started.elapsed() >= self.config.timeout => {
                    child.kill().map_err(|error| {
                        format!("could not stop timed-out shell step `{}`: {error}", step.id)
                    })?;
                    child.wait().map_err(|error| {
                        format!("could not reap timed-out shell step `{}`: {error}", step.id)
                    })?;
                    break None;
                }
                None => thread::sleep(Duration::from_millis(10)),
            }
        };

        let stdout = join_reader(stdout_reader, &step.id)?;
        let stderr = join_reader(stderr_reader, &step.id)?;
        let output =
            redact_known_secrets(format_output(&stdout, &stderr), &self.config.environment);
        match status {
            Some(status) => Ok(exit_output(status, output)),
            None => Ok(StepOutput {
                outcome: "failure".into(),
                output: format!(
                    "command timed out after {:?}\n{output}",
                    self.config.timeout
                ),
                exit_code: None,
                attempts: 1,
                handoff: None,
            }),
        }
    }
}

impl ShellRunner for ShellRunnerAdapter {
    fn run(&mut self, step: &Step) -> Result<StepOutput, String> {
        match &step.kind {
            StepKind::Shell { command, args } => self.execute(step, command, args),
            StepKind::Agent { .. } => Err(format!(
                "shell runner cannot execute agent step `{}`",
                step.id
            )),
        }
    }
}

fn substitute_inputs(
    argument: &str,
    inputs: &HashMap<String, String>,
    step_id: &str,
) -> Result<String, String> {
    let mut result = String::with_capacity(argument.len());
    let mut remainder = argument;
    while let Some(start) = remainder.find("{{") {
        result.push_str(&remainder[..start]);
        let value_start = start + 2;
        let Some(end) = remainder[value_start..].find("}}") else {
            return Err(format!(
                "shell step `{step_id}` has an unterminated input placeholder"
            ));
        };
        let name = &remainder[value_start..value_start + end];
        let value = inputs.get(name).ok_or_else(|| {
            format!("shell step `{step_id}` references missing pipeline input `{name}`")
        })?;
        result.push_str(value);
        remainder = &remainder[value_start + end + 2..];
    }
    result.push_str(remainder);
    Ok(result)
}

fn read_all(mut reader: impl Read) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn join_reader(
    reader: thread::JoinHandle<std::io::Result<Vec<u8>>>,
    step_id: &str,
) -> Result<Vec<u8>, String> {
    reader
        .join()
        .map_err(|_| format!("output reader panicked for shell step `{step_id}`"))?
        .map_err(|error| format!("could not read output for shell step `{step_id}`: {error}"))
}

fn format_output(stdout: &[u8], stderr: &[u8]) -> String {
    let stdout = String::from_utf8_lossy(stdout);
    let stderr = String::from_utf8_lossy(stderr);
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => String::new(),
        (false, true) => stdout.into_owned(),
        (true, false) => format!("stderr:\n{stderr}"),
        (false, false) => format!("{stdout}\nstderr:\n{stderr}"),
    }
}

fn redact_known_secrets(output: String, environment: &HashMap<OsString, OsString>) -> String {
    environment.iter().fold(output, |output, (key, value)| {
        let key = key.to_string_lossy().to_ascii_uppercase();
        let value = value.to_string_lossy();
        if value.len() >= 4
            && ["TOKEN", "KEY", "SECRET", "PASSWORD"]
                .iter()
                .any(|marker| key.contains(marker))
        {
            output.replace(value.as_ref(), "[REDACTED]")
        } else {
            output
        }
    })
}

fn exit_output(status: ExitStatus, output: String) -> StepOutput {
    let exit_code = status.code();
    StepOutput {
        outcome: if status.success() {
            "success"
        } else {
            "failure"
        }
        .into(),
        output,
        exit_code,
        attempts: 1,
        handoff: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use factory_core::StepKind;

    fn shell_step(command: &str, args: &[&str]) -> Step {
        Step {
            id: "shell".into(),
            kind: StepKind::Shell {
                command: command.into(),
                args: args.iter().map(|arg| (*arg).into()).collect(),
            },
            transitions: vec![],
        }
    }

    #[cfg(unix)]
    #[test]
    fn executes_with_configured_directory_environment_and_exit_code() {
        let directory = std::env::current_dir().unwrap();
        let mut config = ShellRunnerConfig::new(directory.clone(), Duration::from_secs(2));
        config
            .environment
            .insert("FACTORY_TEST_VALUE".into(), "controlled".into());
        let mut runner = ShellRunnerAdapter::new(config);
        let step = shell_step(
            "sh",
            &[
                "-c",
                "printf '%s:%s' \"$PWD\" \"$FACTORY_TEST_VALUE\"; exit 7",
            ],
        );
        let output = runner.run(&step).unwrap();
        assert_eq!(output.outcome, "failure");
        assert_eq!(output.exit_code, Some(7));
        assert_eq!(output.output, format!("{}:controlled", directory.display()));
    }

    #[cfg(unix)]
    #[test]
    fn expands_pipeline_input_as_a_single_safe_process_argument() {
        let directory =
            std::env::temp_dir().join(format!("factory-shell-arg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        let marker = directory.join("injected");
        let mut config = ShellRunnerConfig::new(&directory, Duration::from_secs(2));
        config.inputs.insert(
            "target".into(),
            format!("https://example.test/a; touch {}", marker.display()),
        );
        let mut runner = ShellRunnerAdapter::new(config);
        let output = runner
            .run(&shell_step(
                "sh",
                &["-c", "printf '%s' \"$1\"", "sh", "{{target}}"],
            ))
            .unwrap();
        assert_eq!(output.outcome, "success");
        assert!(output.output.contains("; touch"));
        assert!(!marker.exists());
        let _ = std::fs::remove_dir_all(directory);
    }

    #[cfg(unix)]
    #[test]
    fn invokes_fake_command_with_pipeline_input_as_a_separate_argument() {
        use std::os::unix::fs::PermissionsExt;

        let directory =
            std::env::temp_dir().join(format!("factory-fake-gh-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        let executable = directory.join("gh");
        std::fs::write(&executable, "#!/bin/sh\nprintf '%s\\n' \"$@\"\n").unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();

        let target = "https://github.com/a/b/issues/8; $(echo unsafe)";
        let mut config = ShellRunnerConfig::new(&directory, Duration::from_secs(2));
        config
            .environment
            .insert("PATH".into(), directory.as_os_str().into());
        config.inputs.insert("target".into(), target.into());
        let mut runner = ShellRunnerAdapter::new(config);
        let step = shell_step(
            "gh",
            &[
                "issue",
                "view",
                "{{target}}",
                "--json",
                "url,title,body,labels",
            ],
        );
        let output = runner.run(&step).unwrap();
        assert_eq!(output.outcome, "success");
        assert_eq!(
            output.output,
            format!("issue\nview\n{target}\n--json\nurl,title,body,labels\n")
        );
        let _ = std::fs::remove_dir_all(directory);
    }

    #[cfg(unix)]
    #[test]
    fn redacts_sensitive_environment_values_from_step_output() {
        let mut config = ShellRunnerConfig::new(".", Duration::from_secs(2));
        config
            .environment
            .insert("GH_TOKEN".into(), "sensitive-test-token".into());
        let mut runner = ShellRunnerAdapter::new(config);
        let output = runner
            .run(&shell_step("sh", &["-c", "printf '%s' \"$GH_TOKEN\""]))
            .unwrap();
        assert_eq!(output.outcome, "success");
        assert_eq!(output.output, "[REDACTED]");
        assert!(!output.output.contains("sensitive-test-token"));
    }

    #[cfg(unix)]
    #[test]
    fn terminates_processes_that_exceed_the_timeout() {
        let config = ShellRunnerConfig::new(".", Duration::from_millis(30));
        let mut runner = ShellRunnerAdapter::new(config);
        let output = runner
            .run(&shell_step("sh", &["-c", "exec sleep 2"]))
            .unwrap();
        assert_eq!(output.outcome, "failure");
        assert_eq!(output.exit_code, None);
        assert!(output.output.contains("timed out"));
    }
}
