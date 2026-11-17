//! One-shot Pi CLI executor for configured agent steps.

use std::{
    collections::HashMap,
    io::Read,
    path::PathBuf,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use factory_core::{AgentDefinition, Step, StepKind, ThinkingLevel, features::StepOutput};
use serde_json::Value;

use crate::agent::AgentExecutor;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Executes each agent step in a separate Pi process, reusing that agent's run memory.
pub struct PiAgentExecutor {
    memory_paths: HashMap<String, PathBuf>,
    executable: PathBuf,
    timeout: Duration,
}

impl PiAgentExecutor {
    pub fn new(memory_paths: HashMap<String, PathBuf>) -> Self {
        Self {
            memory_paths,
            executable: std::env::var_os("PI_CLI")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("pi")),
            timeout: DEFAULT_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_command(memory_paths: HashMap<String, PathBuf>, executable: PathBuf) -> Self {
        Self {
            memory_paths,
            executable,
            timeout: Duration::from_secs(5),
        }
    }
}

impl AgentExecutor for PiAgentExecutor {
    fn execute(&mut self, agent: &AgentDefinition, step: &Step) -> Result<StepOutput, String> {
        if agent.harness != "pi" {
            return Err(format!(
                "unsupported harness `{}` for agent `{}`",
                agent.harness, agent.name
            ));
        }
        let StepKind::Agent { agent: step_agent } = &step.kind else {
            return Err(format!("step `{}` is not an agent step", step.id));
        };
        if step_agent != &agent.name {
            return Err(format!(
                "step `{}` belongs to `{step_agent}`, not `{}`",
                step.id, agent.name
            ));
        }
        let memory = self
            .memory_paths
            .get(&agent.name)
            .ok_or_else(|| format!("no run memory registered for agent `{}`", agent.name))?;
        if !memory.is_file() {
            return Err(format!(
                "run memory for `{}` is not a readable file: {}",
                agent.name,
                memory.display()
            ));
        }
        let prompt = format!(
            "{}\n\nFactory pipeline step: `{}` (agent `{}`). Read the run context at `{}` before acting and update that same file with your findings and decisions.\n\nWhen finished, your final response MUST be exactly one JSON object with string keys `outcome` and `output`, for example {{\"outcome\":\"success\",\"output\":\"Implemented changes\"}}. Do not wrap it in markdown. Allowed outcomes for this agent are: {}.",
            agent.prompt,
            step.id,
            agent.name,
            memory.display(),
            allowed_outcomes(&agent.name).join(", ")
        );
        let mut command = Command::new(&self.executable);
        command
            .args([
                "--mode",
                "json",
                "--no-session",
                "--no-skills",
                "--no-extensions",
                "--model",
            ])
            .arg(format!("{}/{}", agent.model.provider, agent.model.model));
        if let Some(level) = agent.thinking_level {
            command.args(["--thinking", thinking_arg(level)]);
        }
        command
            .arg(prompt)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command.spawn().map_err(|error| {
            format!(
                "could not start Pi (`{}`): {error}",
                self.executable.display()
            )
        })?;
        let mut stdout = child.stdout.take().expect("piped stdout");
        let mut stderr = child.stderr.take().expect("piped stderr");
        let stdout_reader = thread::spawn(move || {
            let mut value = Vec::new();
            stdout.read_to_end(&mut value).map(|_| value)
        });
        let stderr_reader = thread::spawn(move || {
            let mut value = Vec::new();
            stderr.read_to_end(&mut value).map(|_| value)
        });
        let started = Instant::now();
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if started.elapsed() < self.timeout => {
                    thread::sleep(Duration::from_millis(100))
                }
                Ok(None) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = stdout_reader.join();
                    let _ = stderr_reader.join();
                    return Err(format!(
                        "Pi timed out after {} seconds for step `{}`",
                        self.timeout.as_secs(),
                        step.id
                    ));
                }
                Err(error) => {
                    let _ = child.kill();
                    return Err(format!("could not wait for Pi: {error}"));
                }
            }
        };
        let stdout = stdout_reader
            .join()
            .map_err(|_| "Pi stdout reader panicked".to_owned())?
            .map_err(|error| format!("could not read Pi stdout: {error}"))?;
        let stderr = stderr_reader
            .join()
            .map_err(|_| "Pi stderr reader panicked".to_owned())?
            .map_err(|error| format!("could not read Pi stderr: {error}"))?;
        let stdout = String::from_utf8(stdout)
            .map_err(|error| format!("Pi emitted non-UTF-8 output: {error}"))?;
        let stderr = String::from_utf8_lossy(&stderr).trim().to_owned();
        if !status.success() {
            return Err(format!("Pi exited with {status}{}", diagnostic(&stderr)));
        }
        let response = parse_final_response(&stdout).map_err(|error| {
            format!(
                "Pi returned invalid agent output: {error}{}",
                diagnostic(&stderr)
            )
        })?;
        let allowed = allowed_outcomes(&agent.name);
        if !allowed.contains(&response.outcome.as_str()) {
            return Err(format!(
                "agent `{}` returned unexpected outcome `{}` (expected {})",
                agent.name,
                response.outcome,
                allowed.join(" or ")
            ));
        }
        Ok(StepOutput {
            outcome: response.outcome,
            output: response.output,
            exit_code: status.code(),
        })
    }
}

struct AgentResponse {
    outcome: String,
    output: String,
}

fn parse_final_response(output: &str) -> Result<AgentResponse, String> {
    let mut final_text = None;
    for line in output.lines() {
        let event: Value = serde_json::from_str(line)
            .map_err(|error| format!("invalid Pi JSON event: {error}"))?;
        if event.get("type").and_then(Value::as_str) == Some("agent_settled")
            && event.get("aborted").and_then(Value::as_bool) == Some(true)
        {
            return Err("Pi agent run was aborted".into());
        }
        if event.get("type").and_then(Value::as_str) != Some("message_end") {
            continue;
        }
        let Some(message) = event.get("message") else {
            continue;
        };
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        if matches!(
            message.get("stopReason").and_then(Value::as_str),
            Some("error" | "aborted")
        ) {
            return Err(format!(
                "Pi assistant response ended with `{}`",
                message["stopReason"].as_str().unwrap_or("unknown")
            ));
        }
        let text = message
            .get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<String>();
        if !text.is_empty() {
            final_text = Some(text);
        }
    }
    let text = final_text.ok_or_else(|| "no completed assistant text found".to_owned())?;
    let value: Value = serde_json::from_str(text.trim())
        .map_err(|error| format!("final response must be one JSON object: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "final response must be a JSON object".to_owned())?;
    if object.len() != 2 || !object.contains_key("outcome") || !object.contains_key("output") {
        return Err("final response must contain exactly `outcome` and `output`".into());
    }
    let outcome = object["outcome"]
        .as_str()
        .ok_or_else(|| "`outcome` must be a string".to_owned())?;
    let output = object["output"]
        .as_str()
        .ok_or_else(|| "`output` must be a string".to_owned())?;
    if outcome.is_empty() {
        return Err("`outcome` must not be empty".into());
    }
    Ok(AgentResponse {
        outcome: outcome.into(),
        output: output.into(),
    })
}

fn allowed_outcomes(agent: &str) -> Vec<&'static str> {
    match agent {
        "coder" => vec!["success", "failure"],
        "reviewer" => vec!["approved", "changes_requested"],
        _ => vec!["success", "failure"],
    }
}

fn thinking_arg(level: ThinkingLevel) -> &'static str {
    match level {
        ThinkingLevel::Off => "off",
        ThinkingLevel::Minimal => "minimal",
        ThinkingLevel::Low => "low",
        ThinkingLevel::Medium => "medium",
        ThinkingLevel::High => "high",
        ThinkingLevel::XHigh => "xhigh",
    }
}

fn diagnostic(stderr: &str) -> String {
    if stderr.is_empty() {
        String::new()
    } else {
        format!("\nPi stderr: {stderr}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_only_exact_structured_response_from_final_assistant_event() {
        let transcript = format!(
            "{}\n{}\n",
            json!({"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"old"}]}}),
            json!({"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":r#"{"outcome":"approved","output":"Looks good"}"#}]}})
        );
        let result = parse_final_response(&transcript).unwrap();
        assert_eq!(result.outcome, "approved");
        assert_eq!(result.output, "Looks good");
    }

    #[test]
    fn rejects_unexpected_shapes_and_malformed_json() {
        for value in [
            "not json",
            "{\"outcome\":\"success\"}",
            "{\"outcome\":\"success\",\"output\":\"ok\",\"extra\":1}",
        ] {
            let transcript = json!({"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":value}]}}).to_string();
            assert!(parse_final_response(&transcript).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn forwards_model_thinking_step_and_reusable_memory_to_pi() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!("factory-pi-test-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let memory = root.join("coder.md");
        std::fs::write(&memory, "run context").unwrap();
        let capture = root.join("args.txt");
        let executable = root.join("fake-pi");
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\ncat <<'EVENT'\n{{\"type\":\"message_end\",\"message\":{{\"role\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"{{\\\"outcome\\\":\\\"success\\\",\\\"output\\\":\\\"done\\\"}}\"}}]}}}}\nEVENT\n",
            capture.display()
        );
        std::fs::write(&executable, script).unwrap();
        let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&executable, permissions).unwrap();
        let mut executor = PiAgentExecutor::with_command(
            HashMap::from([("coder".into(), memory.clone())]),
            executable,
        );
        let agent = AgentDefinition {
            name: "coder".into(),
            harness: "pi".into(),
            model: factory_core::ModelRef::parse("provider/model-x").unwrap(),
            thinking_level: Some(ThinkingLevel::High),
            prompt: "Do the task".into(),
        };
        let step = Step {
            id: "implement".into(),
            kind: StepKind::Agent {
                agent: "coder".into(),
            },
            transitions: vec![],
        };
        assert_eq!(executor.execute(&agent, &step).unwrap().outcome, "success");
        let args = std::fs::read_to_string(capture).unwrap();
        assert!(args.contains("provider/model-x"));
        assert!(args.contains("--no-skills"));
        assert!(args.contains("--no-extensions"));
        assert!(args.contains("high"));
        assert!(args.contains("implement"));
        assert!(args.contains("Do the task"));
        assert!(args.contains(memory.to_str().unwrap()));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn requires_memory_and_rejects_wrong_harness_without_starting_process() {
        let mut executor =
            PiAgentExecutor::with_command(HashMap::new(), PathBuf::from("missing-pi"));
        let agent = AgentDefinition {
            name: "coder".into(),
            harness: "pi".into(),
            model: factory_core::ModelRef::parse("test/model").unwrap(),
            thinking_level: None,
            prompt: "do it".into(),
        };
        let step = Step {
            id: "build".into(),
            kind: StepKind::Agent {
                agent: "coder".into(),
            },
            transitions: vec![],
        };
        assert!(
            executor
                .execute(&agent, &step)
                .unwrap_err()
                .contains("no run memory")
        );
    }
}
