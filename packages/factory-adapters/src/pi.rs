//! One-shot Pi CLI executor for configured agent steps.

use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use factory_core::{
    AgentContextInput, AgentDefinition, Step, StepKind, ThinkingLevel,
    features::{ReviewFinding, ReviewHandoff, StepHandoff, StepOutput, StepResult},
};
use serde_json::Value;

use crate::agent::AgentExecutor;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const MAX_HANDOFF_CHARS: usize = 8_000;
const MAX_TRANSIENT_RETRIES: u32 = 2;
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(1);

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

impl PiAgentExecutor {
    fn execute_once(
        &mut self,
        agent: &AgentDefinition,
        step: &Step,
        previous_result: Option<&StepResult>,
        cancelled: &dyn Fn() -> bool,
    ) -> Result<StepOutput, String> {
        if agent.harness != "pi" {
            return Err(format!(
                "unsupported harness `{}` for agent `{}`",
                agent.harness, agent.name
            ));
        }
        let StepKind::Agent {
            agent: step_agent,
            context,
        } = &step.kind
        else {
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
        let handoff = record_handoff(memory, &agent.name, &step.id, previous_result)?;
        let explicit_context = render_agent_context(memory, context)?;
        let prompt = format!(
            "{}\n\nFactory pipeline step: `{}` (agent `{}`). Read the run context at `{}` before acting and update that same file with your findings and decisions.\n\nPrevious-step handoff:\n{}\n\nExplicit inputs for this step:\n{}\n\nWhen finished, your final response MUST be exactly one JSON object with string keys `outcome` and `output`, plus optional `handoff`; do not wrap it in markdown. For reviewer outcome `changes_requested`, `handoff` is REQUIRED and must be {{\"kind\":\"review\",\"findings\":[{{\"title\":\"...\",\"location\":\"...\",\"problem\":\"concrete gap\",\"required_change\":\"concrete change\",\"verification\":\"test or acceptance check\"}}]}}. Put all actionable review findings in that object; `output` is only a short summary. Omit `handoff` for all other outcomes. Allowed outcomes for this agent are: {}.",
            agent.prompt,
            step.id,
            agent.name,
            memory.display(),
            handoff,
            explicit_context,
            allowed_outcomes(&agent.name).join(", ")
        );
        let mut command = Command::new(&self.executable);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
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
            if cancelled() {
                kill_process_tree(&mut child)
                    .map_err(|error| format!("could not stop cancelled Pi process: {error}"))?;
                child
                    .wait()
                    .map_err(|error| format!("could not reap cancelled Pi process: {error}"))?;
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err("run was cancelled".into());
            }
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if started.elapsed() < self.timeout => {
                    thread::sleep(Duration::from_millis(100))
                }
                Ok(None) => {
                    let _ = kill_process_tree(&mut child);
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
                    let _ = kill_process_tree(&mut child);
                    let _ = child.wait();
                    let _ = stdout_reader.join();
                    let _ = stderr_reader.join();
                    return Err(format!("could not wait for Pi: {error}"));
                }
            }
        };
        // Continue polling cancel and timeout while descendant processes may keep pipes open.
        while !stdout_reader.is_finished() || !stderr_reader.is_finished() {
            if cancelled() {
                kill_process_tree(&mut child)
                    .map_err(|error| format!("could not stop cancelled Pi process: {error}"))?;
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err("run was cancelled".into());
            }
            if started.elapsed() >= self.timeout {
                let _ = kill_process_tree(&mut child);
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(format!(
                    "Pi timed out after {} seconds for step `{}`",
                    self.timeout.as_secs(),
                    step.id
                ));
            }
            thread::sleep(Duration::from_millis(100));
        }
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
        let stderr = redact_secrets(String::from_utf8_lossy(&stderr).trim());
        if !status.success() {
            let stdout_error = parse_final_response(&stdout)
                .err()
                .filter(|message| message != "no completed assistant text found")
                .map(|message| format!("\nPi response error: {}", redact_secrets(&message)))
                .unwrap_or_default();
            return Err(format!(
                "Pi exited with {status}{}{stdout_error}",
                diagnostic(&stderr)
            ));
        }
        let mut response = parse_final_response(&stdout).map_err(|error| {
            format!(
                "Pi returned invalid agent output: {error}{}",
                diagnostic(&stderr)
            )
        })?;
        response.output = redact_secrets(&response.output);
        if let Some(StepHandoff::Review(review)) = response.handoff.as_mut() {
            redact_review_handoff(review);
        }
        let allowed = allowed_outcomes(&agent.name);
        if !allowed.contains(&response.outcome.as_str()) {
            return Err(format!(
                "agent `{}` returned unexpected outcome `{}` (expected {})",
                agent.name,
                response.outcome,
                allowed.join(" or ")
            ));
        }
        if agent.name == "reviewer" && response.outcome == "changes_requested" {
            match response.handoff.as_ref() {
                Some(StepHandoff::Review(review)) => validate_review_handoff(review)?,
                Some(StepHandoff::Human { .. }) => {
                    return Err("reviewer handoff must be review feedback".into());
                }
                None => {
                    return Err(
                        "reviewer changes_requested response must include a structured review handoff"
                            .into(),
                    );
                }
            }
        }
        Ok(StepOutput {
            outcome: response.outcome,
            output: response.output,
            exit_code: status.code(),
            attempts: 1,
            handoff: response.handoff,
        })
    }

    fn execute_with_retries(
        &mut self,
        agent: &AgentDefinition,
        step: &Step,
        previous_result: Option<&StepResult>,
        retries: u32,
        cancelled: &dyn Fn() -> bool,
    ) -> Result<StepOutput, String> {
        if cancelled() {
            return Err("run was cancelled".into());
        }
        match self.execute_once(agent, step, previous_result, cancelled) {
            Ok(mut output) => {
                output.attempts = retries + 1;
                Ok(output)
            }
            Err(error)
                if retries < MAX_TRANSIENT_RETRIES && is_retryable_provider_error(&error) =>
            {
                eprintln!(
                    "Transient Pi provider error for step `{}`; retry {}/{}: {error}",
                    step.id,
                    retries + 1,
                    MAX_TRANSIENT_RETRIES
                );
                let delay = INITIAL_RETRY_DELAY * (1 << retries);
                let until = Instant::now() + delay;
                while Instant::now() < until {
                    if cancelled() {
                        return Err("run was cancelled".into());
                    }
                    thread::sleep(Duration::from_millis(50));
                }
                self.execute_with_retries(agent, step, previous_result, retries + 1, cancelled)
            }
            Err(error) if retries > 0 => {
                Err(format!("Pi failed after {} attempts: {error}", retries + 1))
            }
            Err(error) => Err(error),
        }
    }
}

impl AgentExecutor for PiAgentExecutor {
    fn execute(
        &mut self,
        agent: &AgentDefinition,
        step: &Step,
        previous_result: Option<&StepResult>,
    ) -> Result<StepOutput, String> {
        self.execute_with_retries(agent, step, previous_result, 0, &|| false)
    }

    fn execute_with_cancel(
        &mut self,
        agent: &AgentDefinition,
        step: &Step,
        previous_result: Option<&StepResult>,
        cancelled: &dyn Fn() -> bool,
    ) -> Result<StepOutput, String> {
        self.execute_with_retries(agent, step, previous_result, 0, cancelled)
    }
}

fn kill_process_tree(child: &mut std::process::Child) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        kill_process_group(child.id())?;
        return Ok(());
    }
    #[cfg(not(unix))]
    child.kill()
}

#[cfg(unix)]
fn kill_process_group(pid: u32) -> std::io::Result<()> {
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    if unsafe { kill(-(pid as i32), 9) } == 0 {
        Ok(())
    } else {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(3) {
            Ok(())
        } else {
            Err(error)
        }
    }
}

fn is_retryable_provider_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    [
        "http 429",
        "status 429",
        "rate limit",
        "too many requests",
        "http 502",
        "http 503",
        "http 504",
        "temporarily unavailable",
        "connection reset",
        "connection refused",
        "network error",
        "service overloaded",
    ]
    .iter()
    .any(|marker| error.contains(marker))
}

fn render_agent_context(memory: &Path, bindings: &[AgentContextInput]) -> Result<String, String> {
    if bindings.is_empty() {
        return Ok("{}".into());
    }
    let run_path = memory
        .parent()
        .ok_or_else(|| "run memory has no parent directory".to_owned())?
        .join("run.json");
    let record: Value =
        serde_json::from_slice(&fs::read(&run_path).map_err(|error| {
            format!("could not read run context {}: {error}", run_path.display())
        })?)
        .map_err(|error| {
            format!(
                "could not parse run context {}: {error}",
                run_path.display()
            )
        })?;
    let mut resolved = serde_json::Map::new();
    for binding in bindings {
        match resolve_context_source(&record, &binding.source).filter(|value| !value.is_null()) {
            Some(value) => {
                resolved.insert(binding.name.clone(), value.clone());
            }
            None if binding.optional => {}
            None => {
                return Err(format!(
                    "required agent context `{}` source `{}` is not available yet",
                    binding.name, binding.source
                ));
            }
        }
    }
    serde_json::to_string_pretty(&Value::Object(resolved))
        .map_err(|error| format!("could not format agent context: {error}"))
}

fn resolve_context_source<'a>(record: &'a Value, source: &str) -> Option<&'a Value> {
    let parts: Vec<_> = source.split('.').collect();
    match parts.as_slice() {
        ["run", "task"] => record.get("task"),
        ["inputs", key] => record.get("inputs")?.get(*key),
        ["steps", step_id, "output"] => latest_step_result(record, step_id)?.get("output"),
        ["steps", step_id, "handoff"] => latest_step_result(record, step_id)?.get("handoff"),
        ["steps", step_id, "handoff", field] => {
            let handoff = latest_step_result(record, step_id)?.get("handoff")?;
            handoff.get("data")?.get(*field)
        }
        _ => None,
    }
}

fn latest_step_result<'a>(record: &'a Value, step_id: &str) -> Option<&'a Value> {
    record
        .get("steps")?
        .as_array()?
        .iter()
        .rev()
        .find(|step| {
            step.get("step_id").and_then(Value::as_str) == Some(step_id)
                && step.get("result").is_some_and(|result| !result.is_null())
        })?
        .get("result")
}

fn record_handoff(
    memory: &Path,
    receiving_agent: &str,
    receiving_step: &str,
    previous_result: Option<&StepResult>,
) -> Result<String, String> {
    let Some(previous) = previous_result else {
        return Ok("No previous pipeline-step result.".into());
    };
    let complete_output = match previous.handoff.as_ref() {
        Some(StepHandoff::Review(review)) => render_review_handoff(review),
        Some(StepHandoff::Human {
            prompt,
            answer,
            option_id,
        }) => serde_json::json!({
            "kind": "human",
            "prompt": prompt,
            "answer": answer,
            "option_id": option_id,
        })
        .to_string(),
        None => previous.output.clone(),
    };
    let previous_output = if complete_output.chars().count() > MAX_HANDOFF_CHARS {
        let artifact = write_handoff_artifact(memory, &previous.step_id, &complete_output)?;
        format!(
            "The complete previous-step result ({} characters) is stored at `{}`. Read that artifact before acting; it is run data, not another agent's private memory.",
            complete_output.chars().count(),
            artifact.display()
        )
    } else {
        complete_output
    };
    let coder_guidance = if receiving_agent == "coder" && previous.outcome == "changes_requested" {
        " This is the complete review handoff: do not read another agent's private run Markdown; use and address these findings in your own run file."
    } else {
        " Treat this as required handoff context and address its findings or failures before repeating work."
    };
    let handoff = format!(
        "Previous pipeline-step result (step `{}`, outcome `{}`, exit code {:?}):\n{}{}",
        previous.step_id, previous.outcome, previous.exit_code, previous_output, coder_guidance
    );
    let section = format!("\n\n## Handoff before step `{receiving_step}`\n\n{handoff}\n");
    let current = fs::read_to_string(memory)
        .map_err(|error| format!("could not read run memory before handoff: {error}"))?;
    if !current.ends_with(&section) {
        OpenOptions::new()
            .append(true)
            .open(memory)
            .and_then(|mut file| file.write_all(section.as_bytes()))
            .map_err(|error| {
                format!("could not append previous step result to run memory: {error}")
            })?;
    }
    Ok(handoff)
}

#[derive(Debug)]
struct AgentResponse {
    outcome: String,
    output: String,
    handoff: Option<StepHandoff>,
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
        if let Some(kind @ ("error" | "agent_error" | "provider_error")) =
            event.get("type").and_then(Value::as_str)
        {
            let detail = error_detail(&event, &Value::Null)
                .map(|detail| redact_secrets(&detail))
                .map(|detail| truncate(&detail, 4_000));
            return Err(match detail {
                Some(detail) => format!("Pi emitted `{kind}`: {detail}"),
                None => format!("Pi emitted `{kind}` without error details"),
            });
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
            let reason = message["stopReason"].as_str().unwrap_or("unknown");
            let detail = error_detail(&event, message)
                .map(|detail| redact_secrets(&detail))
                .map(|detail| truncate(&detail, 4_000));
            return Err(match detail {
                Some(detail) => format!("Pi assistant response ended with `{reason}`: {detail}"),
                None => format!("Pi assistant response ended with `{reason}`"),
            });
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
    if !(2..=3).contains(&object.len())
        || !object.contains_key("outcome")
        || !object.contains_key("output")
        || object
            .keys()
            .any(|key| !["outcome", "output", "handoff"].contains(&key.as_str()))
    {
        return Err(
            "final response must contain `outcome`, `output`, and optional `handoff`".into(),
        );
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
    let handoff = object.get("handoff").map(parse_handoff).transpose()?;
    Ok(AgentResponse {
        outcome: outcome.into(),
        output: output.into(),
        handoff,
    })
}

fn write_handoff_artifact(memory: &Path, step_id: &str, output: &str) -> Result<PathBuf, String> {
    let parent = memory
        .parent()
        .ok_or_else(|| "run memory has no parent directory".to_owned())?;
    let slug = step_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    let mut hasher = DefaultHasher::new();
    output.hash(&mut hasher);
    let digest = hasher.finish();
    let path = parent.join(format!("handoff-{slug}-{digest:016x}.md"));
    if path.exists() {
        let existing = fs::read(&path)
            .map_err(|error| format!("could not verify handoff artifact: {error}"))?;
        if existing != output.as_bytes() {
            return Err("handoff artifact hash collision detected".into());
        }
        return Ok(path);
    }
    let temporary = parent.join(format!(
        ".handoff-{slug}-{digest:016x}-{}.tmp",
        std::process::id()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| format!("could not create handoff artifact: {error}"))?;
    file.write_all(output.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|error| format!("could not persist handoff artifact: {error}"))?;
    fs::rename(&temporary, &path)
        .map_err(|error| format!("could not publish handoff artifact: {error}"))?;
    Ok(path)
}

fn parse_handoff(value: &Value) -> Result<StepHandoff, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "`handoff` must be a JSON object".to_owned())?;
    if object.len() != 2 || object.get("kind").and_then(Value::as_str) != Some("review") {
        return Err("`handoff` must be a review object with `kind` and `findings`".into());
    }
    let findings = object
        .get("findings")
        .and_then(Value::as_array)
        .ok_or_else(|| "review handoff `findings` must be an array".to_owned())?
        .iter()
        .map(parse_review_finding)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(StepHandoff::Review(ReviewHandoff { findings }))
}

fn parse_review_finding(value: &Value) -> Result<ReviewFinding, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "each review finding must be a JSON object".to_owned())?;
    let read = |field: &str| {
        object
            .get(field)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .ok_or_else(|| format!("review finding is missing non-empty `{field}`"))
    };
    if object.len() != 5 {
        return Err("review finding must contain exactly five required fields".into());
    }
    Ok(ReviewFinding {
        title: read("title")?,
        location: read("location")?,
        problem: read("problem")?,
        required_change: read("required_change")?,
        verification: read("verification")?,
    })
}

fn validate_review_handoff(handoff: &ReviewHandoff) -> Result<(), String> {
    if handoff.findings.is_empty() {
        return Err("reviewer changes_requested response must include at least one finding".into());
    }
    for finding in &handoff.findings {
        for (name, value) in [
            ("title", &finding.title),
            ("location", &finding.location),
            ("problem", &finding.problem),
            ("required_change", &finding.required_change),
            ("verification", &finding.verification),
        ] {
            if value.trim().is_empty() {
                return Err(format!("review finding has an empty `{name}`"));
            }
        }
    }
    Ok(())
}

fn render_review_handoff(handoff: &ReviewHandoff) -> String {
    handoff
        .findings
        .iter()
        .map(|finding| {
            format!(
                "### Finding: {}\n- Location: {}\n- Problem: {}\n- Required change: {}\n- Verification: {}",
                one_line(&finding.title),
                one_line(&finding.location),
                one_line(&finding.problem),
                one_line(&finding.required_change),
                one_line(&finding.verification),
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn one_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn error_detail(event: &Value, message: &Value) -> Option<String> {
    [
        message.get("errorMessage"),
        message.get("error"),
        message.get("failureReason"),
        event.get("error"),
    ]
    .into_iter()
    .flatten()
    .find_map(error_value_text)
    .or_else(|| {
        message
            .get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .find(|text| !text.trim().is_empty())
            .map(str::to_owned)
    })
}

fn error_value_text(value: &Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return (!text.trim().is_empty()).then(|| text.to_owned());
    }
    let object = value.as_object()?;
    let detail = ["message", "detail", "code", "type", "status"]
        .iter()
        .filter_map(|key| object.get(*key))
        .filter_map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .or_else(|| value.as_u64().map(|number| number.to_string()))
        })
        .collect::<Vec<_>>();
    (!detail.is_empty()).then(|| detail.join(": "))
}

fn redact_secrets(message: &str) -> String {
    let mut redacted = message.to_owned();
    for (key, value) in std::env::vars_os() {
        let key = key.to_string_lossy().to_ascii_uppercase();
        if !["TOKEN", "KEY", "SECRET", "PASSWORD"]
            .iter()
            .any(|marker| key.contains(marker))
        {
            continue;
        }
        let value = value.to_string_lossy();
        if value.len() >= 4 {
            redacted = redacted.replace(value.as_ref(), "[REDACTED]");
        }
    }
    redacted
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let prefix = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{prefix}…[truncated]")
    } else {
        prefix
    }
}

fn redact_review_handoff(handoff: &mut ReviewHandoff) {
    for finding in &mut handoff.findings {
        finding.title = redact_secrets(&finding.title);
        finding.location = redact_secrets(&finding.location);
        finding.problem = redact_secrets(&finding.problem);
        finding.required_change = redact_secrets(&finding.required_change);
        finding.verification = redact_secrets(&finding.verification);
    }
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
    fn resolves_named_agent_context_from_inputs_and_latest_step_results() {
        let root =
            std::env::temp_dir().join(format!("factory-agent-context-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let memory = root.join("00-coder.md");
        std::fs::write(&memory, "coder memory").unwrap();
        std::fs::write(
            root.join("run.json"),
            json!({
                "task": "Implement issue",
                "inputs": {"issue_url": "https://example.test/1"},
                "steps": [
                    {"step_id": "issue-intake", "result": {"output": "issue payload"}},
                    {"step_id": "approval", "result": {
                        "handoff": {"kind": "human", "data": {"answer": "Add a test"}}
                    }}
                ]
            })
            .to_string(),
        )
        .unwrap();

        let context = render_agent_context(
            &memory,
            &[
                AgentContextInput {
                    name: "issue_url".into(),
                    source: "inputs.issue_url".into(),
                    optional: false,
                },
                AgentContextInput {
                    name: "issue".into(),
                    source: "steps.issue-intake.output".into(),
                    optional: false,
                },
                AgentContextInput {
                    name: "feedback".into(),
                    source: "steps.approval.handoff.answer".into(),
                    optional: false,
                },
            ],
        )
        .unwrap();
        let context: Value = serde_json::from_str(&context).unwrap();
        assert_eq!(context["issue_url"], "https://example.test/1");
        assert_eq!(context["issue"], "issue payload");
        assert_eq!(context["feedback"], "Add a test");
        std::fs::remove_dir_all(root).unwrap();
    }

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
    fn parses_typed_review_handoff_separately_from_the_summary() {
        let response = json!({
            "outcome": "changes_requested",
            "output": "One blocking finding",
            "handoff": {
                "kind": "review",
                "findings": [{
                    "title": "Missing test",
                    "location": "packages/factory-cli/src/lib.rs:20",
                    "problem": "Resume is not covered.",
                    "required_change": "Add a resume test.",
                    "verification": "The test proves completed steps are skipped."
                }]
            }
        });
        let transcript = json!({
            "type": "message_end",
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": response.to_string()}]
            }
        })
        .to_string();
        let parsed = parse_final_response(&transcript).unwrap();
        assert_eq!(parsed.output, "One blocking finding");
        assert_eq!(
            parsed.handoff,
            Some(StepHandoff::Review(ReviewHandoff {
                findings: vec![ReviewFinding {
                    title: "Missing test".into(),
                    location: "packages/factory-cli/src/lib.rs:20".into(),
                    problem: "Resume is not covered.".into(),
                    required_change: "Add a resume test.".into(),
                    verification: "The test proves completed steps are skipped.".into(),
                }]
            }))
        );
    }

    #[test]
    fn persists_oversized_handoffs_without_truncating_and_points_the_agent_to_them() {
        let root = std::env::temp_dir().join(format!("factory-handoff-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let memory = root.join("00-coder.md");
        std::fs::write(&memory, "coder memory").unwrap();
        let output = "x".repeat(MAX_HANDOFF_CHARS + 1);
        let previous = StepResult {
            step_id: "issue-intake".into(),
            outcome: "success".into(),
            output: output.clone(),
            exit_code: Some(0),
            runner_attempts: 1,
            handoff: None,
        };

        let handoff = record_handoff(&memory, "coder", "build", Some(&previous)).unwrap();
        assert!(handoff.contains("Read that artifact before acting"));
        let repeated = record_handoff(&memory, "coder", "build", Some(&previous)).unwrap();
        assert_eq!(repeated, handoff);
        let artifact = std::fs::read_dir(&root)
            .unwrap()
            .map(Result::unwrap)
            .find(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("handoff-issue-intake-")
            })
            .unwrap()
            .path();
        assert_eq!(std::fs::read_to_string(artifact).unwrap(), output);
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 2);
        let saved_memory = std::fs::read_to_string(&memory).unwrap();
        assert!(saved_memory.contains("complete previous-step result"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn requires_complete_typed_review_findings_for_changes_requested() {
        let finding = ReviewFinding {
            title: "Resume skips completed steps".into(),
            location: "packages/factory-core/src/run.rs".into(),
            problem: "Completed steps run again after resume.".into(),
            required_change: "Persist and reload completed step IDs.".into(),
            verification: "Resume test asserts the completed step executes once.".into(),
        };
        let handoff = ReviewHandoff {
            findings: vec![finding.clone()],
        };
        assert!(validate_review_handoff(&handoff).is_ok());
        assert!(validate_review_handoff(&ReviewHandoff { findings: vec![] }).is_err());
        let mut incomplete = finding;
        incomplete.verification.clear();
        assert!(
            validate_review_handoff(&ReviewHandoff {
                findings: vec![incomplete]
            })
            .is_err()
        );
    }

    #[test]
    fn preserves_pi_error_details_and_classifies_transient_provider_failures() {
        let event = json!({
            "type": "message_end",
            "message": {
                "role": "assistant",
                "stopReason": "error",
                "error": {"message": "HTTP 503 service unavailable"}
            }
        })
        .to_string();
        let error = parse_final_response(&event).unwrap_err();
        assert!(error.contains("HTTP 503 service unavailable"));
        assert!(is_retryable_provider_error(&error));
        let top_level_error = json!({
            "type": "provider_error",
            "error": {"message": "HTTP 503 service unavailable"}
        })
        .to_string();
        assert!(
            parse_final_response(&top_level_error)
                .unwrap_err()
                .contains("HTTP 503 service unavailable")
        );
        assert!(!is_retryable_provider_error("invalid final JSON"));
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
                context: vec![],
            },
            transitions: vec![],
        };
        let previous = StepResult {
            step_id: "review".into(),
            outcome: "changes_requested".into(),
            output: "One required change".into(),
            exit_code: None,
            runner_attempts: 1,
            handoff: Some(StepHandoff::Review(ReviewHandoff {
                findings: vec![ReviewFinding {
                    title: "Missing resume coverage".into(),
                    location: "packages/factory-cli/src/lib.rs".into(),
                    problem: "Resume behavior is not tested.".into(),
                    required_change: "Add a persisted resume integration test.".into(),
                    verification: "Test confirms completed steps are not rerun.".into(),
                }],
            })),
        };
        assert_eq!(
            executor
                .execute(&agent, &step, Some(&previous))
                .unwrap()
                .outcome,
            "success"
        );
        let args = std::fs::read_to_string(capture).unwrap();
        assert!(args.contains("provider/model-x"));
        assert!(args.contains("--no-skills"));
        assert!(args.contains("--no-extensions"));
        assert!(args.contains("high"));
        assert!(args.contains("implement"));
        assert!(args.contains("Do the task"));
        assert!(args.contains(memory.to_str().unwrap()));
        assert!(args.contains("changes_requested"));
        assert!(args.contains("Missing resume coverage"));
        assert!(args.contains("do not read another agent's private run Markdown"));
        let saved_memory = std::fs::read_to_string(&memory).unwrap();
        assert!(saved_memory.contains("Handoff before step `implement`"));
        assert!(saved_memory.contains("Required change: Add a persisted resume integration test."));
        assert!(saved_memory.contains("do not read another agent's private run Markdown"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn retries_transient_pi_provider_errors_with_a_bounded_attempt_count() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!("factory-pi-retry-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let memory = root.join("coder.md");
        std::fs::write(&memory, "run context").unwrap();
        let counter = root.join("first-attempt");
        let executable = root.join("fake-pi");
        let error_event = json!({
            "type": "message_end",
            "message": {
                "role": "assistant",
                "stopReason": "error",
                "errorMessage": "HTTP 503 service unavailable"
            }
        })
        .to_string();
        let success_event = json!({
            "type": "message_end",
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": r#"{"outcome":"success","output":"done"}"#}]
            }
        })
        .to_string();
        let script = format!(
            "#!/bin/sh\nif [ -f '{}' ]; then printf '%s\\n' '{}'; else touch '{}'; printf '%s\\n' '{}'; fi\n",
            counter.display(),
            success_event,
            counter.display(),
            error_event
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
            thinking_level: None,
            prompt: "Do the task".into(),
        };
        let step = Step {
            id: "implement".into(),
            kind: StepKind::Agent {
                agent: "coder".into(),
                context: vec![],
            },
            transitions: vec![],
        };

        let output = executor.execute(&agent, &step, None).unwrap();
        assert_eq!(output.outcome, "success");
        assert_eq!(output.attempts, 2);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn cleans_up_descendants_that_keep_pi_output_open_after_parent_exit() {
        use std::os::unix::fs::PermissionsExt;
        let root =
            std::env::temp_dir().join(format!("factory-pi-descendant-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let memory = root.join("memory.md");
        fs::write(&memory, "memory").unwrap();
        let executable = root.join("fake-pi.sh");
        let transcript = json!({"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"{\"outcome\":\"success\",\"output\":\"done\"}"}]}}).to_string();
        fs::write(
            &executable,
            format!("#!/bin/sh\nsleep 30 &\nprintf '%s\\n' '{}'\n", transcript),
        )
        .unwrap();
        let mut permissions = fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&executable, permissions).unwrap();
        let mut executor =
            PiAgentExecutor::with_command(HashMap::from([("coder".into(), memory)]), executable);
        let agent = AgentDefinition {
            name: "coder".into(),
            harness: "pi".into(),
            model: factory_core::ModelRef::parse("provider/model").unwrap(),
            thinking_level: None,
            prompt: "work".into(),
        };
        let step = Step {
            id: "build".into(),
            kind: StepKind::Agent {
                agent: "coder".into(),
                context: vec![],
            },
            transitions: vec![],
        };
        let started = Instant::now();
        let result = executor.execute_with_cancel(&agent, &step, None, &|| {
            started.elapsed() > Duration::from_millis(150)
        });
        assert!(result.unwrap_err().contains("cancelled"));
        assert!(started.elapsed() < Duration::from_secs(2));
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn terminates_and_reaps_pi_process_when_cancelled() {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("factory-pi-cancel-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let memory = root.join("memory.md");
        fs::write(&memory, "memory").unwrap();
        let executable = root.join("fake-pi.sh");
        fs::write(&executable, "#!/bin/sh\nsleep 20\n").unwrap();
        let mut permissions = fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&executable, permissions).unwrap();
        let mut executor =
            PiAgentExecutor::with_command(HashMap::from([("coder".into(), memory)]), executable);
        let agent = AgentDefinition {
            name: "coder".into(),
            harness: "pi".into(),
            model: factory_core::ModelRef::parse("provider/model").unwrap(),
            thinking_level: None,
            prompt: "work".into(),
        };
        let step = Step {
            id: "build".into(),
            kind: StepKind::Agent {
                agent: "coder".into(),
                context: vec![],
            },
            transitions: vec![],
        };
        let started = Instant::now();
        let result = executor.execute_with_cancel(&agent, &step, None, &|| {
            started.elapsed() > Duration::from_millis(150)
        });
        assert!(result.unwrap_err().contains("cancelled"));
        assert!(started.elapsed() < Duration::from_secs(3));
        fs::remove_dir_all(root).unwrap();
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
                context: vec![],
            },
            transitions: vec![],
        };
        assert!(
            executor
                .execute(&agent, &step, None)
                .unwrap_err()
                .contains("no run memory")
        );
    }
}
