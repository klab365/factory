//! Harness-independent rendering and persistence of pipeline handoffs in run memories.

use std::{
    fs::{self, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io::Write,
    path::{Path, PathBuf},
};

use factory_core::features::{ReviewHandoff, StepHandoff, StepResult};

pub(crate) const MAX_HANDOFF_CHARS: usize = 8_000;

/// Records the previous pipeline result in a receiving agent's run memory and returns
/// the same handoff text for use by any agent harness.
pub fn record_handoff(
    memory: &Path,
    receiving_agent: &str,
    receiving_step: &str,
    previous_result: Option<&StepResult>,
) -> Result<String, String> {
    let Some(previous) = previous_result else {
        return Ok("No previous pipeline-step result.".into());
    };
    let current = fs::read_to_string(memory)
        .map_err(|error| format!("could not read run memory before handoff: {error}"))?;
    let handoff_id = next_markdown_id(&current, "## ", ". Handoff:");
    let complete_output = match previous.handoff.as_ref() {
        Some(StepHandoff::Review(review)) => render_review_handoff(review, handoff_id),
        Some(StepHandoff::Human {
            prompt,
            answer,
            option_id,
        }) => serde_json::json!({
            "kind": "human", "prompt": prompt, "answer": answer, "option_id": option_id,
        })
        .to_string(),
        None => previous.output.clone(),
    };
    let previous_output = if complete_output.chars().count() > MAX_HANDOFF_CHARS {
        let artifact = write_handoff_artifact(memory, &previous.step_id, &complete_output)?;
        let finding_index = match previous.handoff.as_ref() {
            Some(StepHandoff::Review(review)) => review
                .findings
                .iter()
                .enumerate()
                .map(|(index, finding)| {
                    format!("- Finding {handoff_id}.{}: {}", index + 1, finding.title)
                })
                .collect::<Vec<_>>()
                .join("\n"),
            _ => String::new(),
        };
        format!(
            "The complete previous-step result ({} characters) is stored at `{}`. Read that artifact before acting; it is run data, not another agent's private memory.\n{}",
            complete_output.chars().count(),
            artifact.display(),
            finding_index
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
    let attempt = run_step_attempt_number(memory, &previous.step_id).max(1);
    let section = format!(
        "\n\n## {handoff_id}. Handoff: Step `{}` → Agent `{receiving_agent}` / Step `{receiving_step}` (Versuch {attempt})\n\n{handoff}\n",
        previous.step_id
    );
    OpenOptions::new()
        .append(true)
        .open(memory)
        .and_then(|mut file| file.write_all(section.as_bytes()))
        .map_err(|error| format!("could not append previous step result to run memory: {error}"))?;
    Ok(handoff)
}

pub(crate) fn run_step_attempt_number(memory_path: &Path, step_id: &str) -> usize {
    let Some(run_dir) = memory_path.parent() else {
        return 0;
    };
    let Ok(contents) = fs::read(run_dir.join("run.json")) else {
        return 0;
    };
    let Ok(record) = serde_json::from_slice::<serde_json::Value>(&contents) else {
        return 0;
    };
    record
        .get("steps")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|attempt| {
            attempt.get("step_id").and_then(serde_json::Value::as_str) == Some(step_id)
        })
        .count()
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

pub(crate) fn render_review_handoff(handoff: &ReviewHandoff, handoff_id: usize) -> String {
    handoff.findings.iter().enumerate().map(|(index, finding)| format!(
        "### {handoff_id}.{} {}\n- Location: {}\n- Problem: {}\n- Required change: {}\n- Verification: {}",
        index + 1, one_line(&finding.title), one_line(&finding.location), one_line(&finding.problem),
        one_line(&finding.required_change), one_line(&finding.verification),
    )).collect::<Vec<_>>().join("\n\n")
}

fn next_markdown_id(contents: &str, prefix: &str, marker: &str) -> usize {
    contents
        .lines()
        .filter_map(|line| line.strip_prefix(prefix))
        .filter_map(|line| line.split_once(marker).map(|(id, _)| id))
        .filter_map(|id| id.parse::<usize>().ok())
        .max()
        .unwrap_or(0)
        + 1
}

fn one_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}
