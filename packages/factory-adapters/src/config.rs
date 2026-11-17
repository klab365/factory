//! TOML configuration loading and validation for factory-core domain types.

use std::{
    collections::HashSet,
    error::Error,
    fmt,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use factory_core::{
    AgentDefinition, ModelRef, Pipeline, PipelineInput, Step, StepKind, ThinkingLevel, Transition,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Configuration {
    pub agents: Vec<AgentDefinition>,
    pub pipelines: Vec<Pipeline>,
}

#[derive(Debug)]
pub struct ConfigError {
    pub path: PathBuf,
    pub message: String,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path.display(), self.message)
    }
}

impl Error for ConfigError {}

/// Loads and validates all agent and pipeline TOML files beneath `.factory/`.
pub fn load_configuration(root: impl AsRef<Path>) -> Result<Configuration, ConfigError> {
    let root = root.as_ref();
    let agent_dir = root.join(".factory/agents");
    let pipeline_dir = root.join(".factory/pipelines");
    let agent_paths = toml_files(&agent_dir)?;
    let pipeline_paths = toml_files(&pipeline_dir)?;

    let mut agents = Vec::with_capacity(agent_paths.len());
    let mut agent_names = HashSet::new();
    for path in agent_paths {
        let dto: AgentDto = parse_file(&path)?;
        let agent = validate_agent(dto, &path)?;
        if !agent_names.insert(agent.name.clone()) {
            return Err(invalid(
                &path,
                format!("duplicate agent name `{}`", agent.name),
            ));
        }
        agents.push(agent);
    }

    let mut pipelines = Vec::with_capacity(pipeline_paths.len());
    for path in pipeline_paths {
        let dto: PipelineDto = parse_file(&path)?;
        pipelines.push(validate_pipeline(dto, &path, &agents)?);
    }

    Ok(Configuration { agents, pipelines })
}

/// Writes a JSON snapshot of a validated configuration, then reloads it from disk.
/// The returned value is the one callers should use for the remainder of the run.
pub fn write_and_load_snapshot(
    configuration: &Configuration,
    path: impl AsRef<Path>,
) -> Result<Configuration, ConfigError> {
    let path = path.as_ref();
    let snapshot = ConfigSnapshotDto {
        version: 1,
        agents: configuration.agents.iter().map(agent_to_snapshot).collect(),
        pipelines: configuration
            .pipelines
            .iter()
            .map(pipeline_to_dto)
            .collect(),
    };
    let json = serde_json::to_vec_pretty(&snapshot).map_err(|error| ConfigError {
        path: path.to_owned(),
        message: format!("could not serialize configuration snapshot: {error}"),
    })?;
    write_atomic(path, &json).map_err(|error| ConfigError {
        path: path.to_owned(),
        message: format!("could not atomically write configuration snapshot: {error}"),
    })?;
    load_configuration_snapshot(path)
}

/// Loads and semantically validates a previously written JSON configuration snapshot.
pub fn load_configuration_snapshot(path: impl AsRef<Path>) -> Result<Configuration, ConfigError> {
    let path = path.as_ref();
    let source = fs::read_to_string(path).map_err(|error| ConfigError {
        path: path.to_owned(),
        message: format!("could not read configuration snapshot: {error}"),
    })?;
    let snapshot: ConfigSnapshotDto =
        serde_json::from_str(&source).map_err(|error| ConfigError {
            path: path.to_owned(),
            message: format!("invalid JSON configuration snapshot: {error}"),
        })?;
    if snapshot.version != 1 {
        return Err(invalid(
            path,
            format!(
                "unsupported configuration snapshot version `{}`",
                snapshot.version
            ),
        ));
    }

    let mut agents = Vec::with_capacity(snapshot.agents.len());
    let mut agent_names = HashSet::new();
    for dto in snapshot.agents {
        let agent = validate_snapshot_agent(dto, path)?;
        if !agent_names.insert(agent.name.clone()) {
            return Err(invalid(
                path,
                format!("duplicate agent name `{}`", agent.name),
            ));
        }
        agents.push(agent);
    }
    let pipelines = snapshot
        .pipelines
        .into_iter()
        .map(|dto| validate_pipeline(dto, path, &agents))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Configuration { agents, pipelines })
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let temporary = parent.join(format!(".snapshot-{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    fs::File::open(parent)?.sync_all()
}

fn toml_files(dir: &Path) -> Result<Vec<PathBuf>, ConfigError> {
    let entries = fs::read_dir(dir).map_err(|error| ConfigError {
        path: dir.to_owned(),
        message: format!("could not read configuration directory: {error}"),
    })?;
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| ConfigError {
            path: dir.to_owned(),
            message: format!("could not read directory entry: {error}"),
        })?;
        let path = entry.path();
        if path.is_file()
            && path
                .extension()
                .is_some_and(|extension| extension == "toml")
        {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn parse_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, ConfigError> {
    let source = fs::read_to_string(path).map_err(|error| ConfigError {
        path: path.to_owned(),
        message: format!("could not read file: {error}"),
    })?;
    toml::from_str(&source).map_err(|error| ConfigError {
        path: path.to_owned(),
        message: format!("invalid TOML: {error}"),
    })
}

#[derive(Serialize, Deserialize)]
struct ConfigSnapshotDto {
    version: u32,
    agents: Vec<AgentSnapshotDto>,
    pipelines: Vec<PipelineDto>,
}

#[derive(Serialize, Deserialize)]
struct AgentSnapshotDto {
    name: String,
    harness: String,
    model: String,
    thinking_level: Option<String>,
    prompt: String,
}

fn agent_to_snapshot(agent: &AgentDefinition) -> AgentSnapshotDto {
    AgentSnapshotDto {
        name: agent.name.clone(),
        harness: agent.harness.clone(),
        model: format!("{}/{}", agent.model.provider, agent.model.model),
        thinking_level: agent
            .thinking_level
            .map(thinking_level_name)
            .map(str::to_owned),
        prompt: agent.prompt.clone(),
    }
}

fn thinking_level_name(level: ThinkingLevel) -> &'static str {
    match level {
        ThinkingLevel::Off => "off",
        ThinkingLevel::Minimal => "minimal",
        ThinkingLevel::Low => "low",
        ThinkingLevel::Medium => "medium",
        ThinkingLevel::High => "high",
        ThinkingLevel::XHigh => "xhigh",
    }
}

fn pipeline_to_dto(pipeline: &Pipeline) -> PipelineDto {
    PipelineDto {
        name: pipeline.name.clone(),
        inputs: pipeline
            .inputs
            .iter()
            .map(|input| InputDto {
                key: input.key.clone(),
                description: input.description.clone(),
            })
            .collect(),
        steps: pipeline
            .steps
            .iter()
            .map(|step| {
                let transitions = step
                    .transitions
                    .iter()
                    .map(|transition| TransitionDto {
                        outcome: transition.outcome.clone(),
                        target_step: transition.target_step.clone(),
                    })
                    .collect();
                match &step.kind {
                    StepKind::Agent { agent } => StepDto::Agent {
                        id: step.id.clone(),
                        agent: agent.clone(),
                        transitions,
                    },
                    StepKind::Shell { command, args } => StepDto::Shell {
                        id: step.id.clone(),
                        command: command.clone(),
                        args: args.clone(),
                        transitions,
                    },
                }
            })
            .collect(),
    }
}

#[derive(Deserialize)]
struct AgentDto {
    name: String,
    harness: String,
    model: String,
    thinking_level: String,
    prompt: String,
}

fn validate_agent(dto: AgentDto, path: &Path) -> Result<AgentDefinition, ConfigError> {
    validate_agent_fields(
        dto.name,
        dto.harness,
        dto.model,
        Some(dto.thinking_level),
        dto.prompt,
        path,
    )
}

fn validate_snapshot_agent(
    dto: AgentSnapshotDto,
    path: &Path,
) -> Result<AgentDefinition, ConfigError> {
    validate_agent_fields(
        dto.name,
        dto.harness,
        dto.model,
        dto.thinking_level,
        dto.prompt,
        path,
    )
}

fn validate_agent_fields(
    name: String,
    harness: String,
    model_value: String,
    thinking_value: Option<String>,
    prompt: String,
    path: &Path,
) -> Result<AgentDefinition, ConfigError> {
    if name.trim().is_empty() {
        return Err(invalid(path, "field `name` must not be empty"));
    }
    if harness.trim().is_empty() {
        return Err(invalid(path, "field `harness` must not be empty"));
    }
    if prompt.trim().is_empty() {
        return Err(invalid(
            path,
            format!("agent `{name}` field `prompt` must not be empty"),
        ));
    }
    let model = ModelRef::parse(&model_value)
        .map_err(|error| invalid(path, format!("agent `{name}` field `model`: {error}")))?;
    if model_value.chars().any(char::is_whitespace) {
        return Err(invalid(
            path,
            format!("agent `{}` field `model` must not contain whitespace", name),
        ));
    }
    let thinking_level = thinking_value
        .map(|value| {
            parse_thinking_level(&value).ok_or_else(|| {
                invalid(
                    path,
                    format!("agent `{name}` has invalid field `thinking_level`: `{value}`"),
                )
            })
        })
        .transpose()?;

    Ok(AgentDefinition {
        name,
        harness,
        model,
        thinking_level,
        prompt,
    })
}

fn parse_thinking_level(value: &str) -> Option<ThinkingLevel> {
    match value {
        "off" => Some(ThinkingLevel::Off),
        "minimal" => Some(ThinkingLevel::Minimal),
        "low" => Some(ThinkingLevel::Low),
        "medium" => Some(ThinkingLevel::Medium),
        "high" => Some(ThinkingLevel::High),
        "xhigh" => Some(ThinkingLevel::XHigh),
        _ => None,
    }
}

#[derive(Serialize, Deserialize)]
struct PipelineDto {
    name: String,
    #[serde(default)]
    inputs: Vec<InputDto>,
    steps: Vec<StepDto>,
}

#[derive(Serialize, Deserialize)]
struct InputDto {
    key: String,
    description: String,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum StepDto {
    Agent {
        id: String,
        agent: String,
        #[serde(default)]
        transitions: Vec<TransitionDto>,
    },
    Shell {
        id: String,
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        transitions: Vec<TransitionDto>,
    },
}

#[derive(Serialize, Deserialize)]
struct TransitionDto {
    outcome: String,
    target_step: String,
}

fn validate_pipeline(
    dto: PipelineDto,
    path: &Path,
    agents: &[AgentDefinition],
) -> Result<Pipeline, ConfigError> {
    if dto.name.trim().is_empty() {
        return Err(invalid(path, "field `name` must not be empty"));
    }
    let mut input_names = HashSet::new();
    let mut inputs = Vec::with_capacity(dto.inputs.len());
    for input in dto.inputs {
        if input.key.trim().is_empty()
            || !input
                .key
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            return Err(invalid(
                path,
                format!(
                    "pipeline `{}` has invalid input key `{}`",
                    dto.name, input.key
                ),
            ));
        }
        if input.description.trim().is_empty() {
            return Err(invalid(
                path,
                format!(
                    "pipeline `{}` input `{}` needs a description",
                    dto.name, input.key
                ),
            ));
        }
        if !input_names.insert(input.key.clone()) {
            return Err(invalid(
                path,
                format!(
                    "pipeline `{}` has duplicate input `{}`",
                    dto.name, input.key
                ),
            ));
        }
        inputs.push(PipelineInput {
            key: input.key,
            description: input.description,
        });
    }
    if dto.steps.is_empty() {
        return Err(invalid(
            path,
            format!("pipeline `{}` must contain at least one step", dto.name),
        ));
    }

    let agent_names: HashSet<&str> = agents.iter().map(|agent| agent.name.as_str()).collect();
    let mut step_ids = HashSet::new();
    let mut steps = Vec::with_capacity(dto.steps.len());
    for step in dto.steps {
        let (id, kind, transitions) = match step {
            StepDto::Agent {
                id,
                agent,
                transitions,
            } => {
                if !agent_names.contains(agent.as_str()) {
                    return Err(invalid(
                        path,
                        format!(
                            "pipeline `{}` step `{id}` references unknown agent `{agent}`",
                            dto.name
                        ),
                    ));
                }
                (id, StepKind::Agent { agent }, transitions)
            }
            StepDto::Shell {
                id,
                command,
                args,
                transitions,
            } => {
                if command.trim().is_empty() {
                    return Err(invalid(
                        path,
                        format!(
                            "pipeline `{}` step `{id}` field `command` must not be empty",
                            dto.name
                        ),
                    ));
                }
                (id, StepKind::Shell { command, args }, transitions)
            }
        };
        if id.trim().is_empty() {
            return Err(invalid(
                path,
                format!("pipeline `{}` has a step with an empty `id`", dto.name),
            ));
        }
        if !step_ids.insert(id.clone()) {
            return Err(invalid(
                path,
                format!("pipeline `{}` has duplicate step id `{id}`", dto.name),
            ));
        }
        let transitions = transitions
            .into_iter()
            .map(|transition| Transition {
                outcome: transition.outcome,
                target_step: transition.target_step,
            })
            .collect();
        steps.push(Step {
            id,
            kind,
            transitions,
        });
    }

    for step in &steps {
        for transition in &step.transitions {
            if !step_ids.contains(&transition.target_step) {
                return Err(invalid(
                    path,
                    format!(
                        "pipeline `{}` step `{}` transition `{}` targets unknown step `{}`",
                        dto.name, step.id, transition.outcome, transition.target_step
                    ),
                ));
            }
        }
    }

    Ok(Pipeline {
        name: dto.name,
        inputs,
        steps,
    })
}

fn invalid(path: &Path, message: impl Into<String>) -> ConfigError {
    ConfigError {
        path: path.to_owned(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        sync::atomic::{AtomicUsize, Ordering},
    };

    static NEXT_TEMP_PROJECT: AtomicUsize = AtomicUsize::new(0);

    struct TempProject(PathBuf);

    impl TempProject {
        fn new() -> Self {
            let unique = NEXT_TEMP_PROJECT.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "factory-adapters-config-{}-{unique}",
                std::process::id()
            ));
            fs::create_dir_all(path.join(".factory/agents")).unwrap();
            fs::create_dir_all(path.join(".factory/pipelines")).unwrap();
            Self(path)
        }

        fn file(&self, relative: &str, content: &str) -> PathBuf {
            let path = self.0.join(relative);
            fs::write(&path, content).unwrap();
            path
        }
    }

    impl Drop for TempProject {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    const AGENT: &str = "name = 'coder'\nharness = 'pi'\nmodel = 'openai-codex/gpt-6-luna'\nthinking_level = 'high'\nprompt = 'Implement the task.'\n";

    #[test]
    fn loads_agent_and_pipeline_into_core_types() {
        let project = TempProject::new();
        project.file(".factory/agents/coder.toml", AGENT);
        project.file(".factory/pipelines/feature.toml", "name = 'feature'\n[[inputs]]\nkey = 'target'\ndescription = 'The target value'\n\n[[steps]]\nid = 'build'\ntype = 'agent'\nagent = 'coder'\n\n[[steps.transitions]]\noutcome = 'success'\ntarget_step = 'test'\n\n[[steps]]\nid = 'test'\ntype = 'shell'\ncommand = 'cargo'\nargs = ['test']\n");

        let config = load_configuration(&project.0).unwrap();
        assert_eq!(config.agents[0].name, "coder");
        assert_eq!(config.agents[0].thinking_level, Some(ThinkingLevel::High));
        assert_eq!(config.pipelines[0].inputs[0].key, "target");
        assert_eq!(
            config.pipelines[0].steps[0].kind,
            StepKind::Agent {
                agent: "coder".into()
            }
        );
        assert_eq!(
            config.pipelines[0].steps[0].transitions[0].target_step,
            "test"
        );
        assert_eq!(
            config.pipelines[0].steps[1].kind,
            StepKind::Shell {
                command: "cargo".into(),
                args: vec!["test".into()]
            }
        );
    }

    #[test]
    fn snapshots_configuration_as_json_and_reloads_the_saved_copy() {
        let project = TempProject::new();
        let agent_path = project.file(".factory/agents/coder.toml", AGENT);
        project.file(
            ".factory/pipelines/feature.toml",
            "name = 'feature'\n\n[[steps]]\nid = 'build'\ntype = 'agent'\nagent = 'coder'\n",
        );
        let original = load_configuration(&project.0).unwrap();
        let run_dir = project.0.join(".factory/runs/run-1");
        fs::create_dir_all(&run_dir).unwrap();
        let snapshot_path = run_dir.join("config.json");

        let snapshot_config = write_and_load_snapshot(&original, &snapshot_path).unwrap();
        assert_eq!(snapshot_config, original);
        let json = fs::read_to_string(&snapshot_path).unwrap();
        assert!(json.contains("\"version\": 1"));

        fs::write(
            agent_path,
            AGENT.replace("Implement the task.", "Changed after run start."),
        )
        .unwrap();
        assert_eq!(
            load_configuration_snapshot(&snapshot_path).unwrap(),
            original
        );
    }

    #[test]
    fn reports_toml_syntax_errors_with_the_file_path() {
        let project = TempProject::new();
        let path = project.file(".factory/agents/broken.toml", "name = [");
        let error = load_configuration(&project.0).unwrap_err();
        assert!(error.to_string().contains(path.to_str().unwrap()));
        assert!(error.message.contains("invalid TOML"));
    }

    #[test]
    fn reports_semantic_reference_errors_with_pipeline_and_step() {
        let project = TempProject::new();
        project.file(".factory/agents/coder.toml", AGENT);
        project.file(
            ".factory/pipelines/feature.toml",
            "name = 'feature'\n\n[[steps]]\nid = 'build'\ntype = 'agent'\nagent = 'missing'\n",
        );
        let error = load_configuration(&project.0).unwrap_err();
        assert!(error.message.contains("pipeline `feature` step `build`"));
        assert!(error.message.contains("unknown agent `missing`"));
    }

    #[test]
    fn rejects_duplicate_agent_names() {
        let project = TempProject::new();
        project.file(".factory/agents/a.toml", AGENT);
        project.file(".factory/agents/b.toml", AGENT);
        let error = load_configuration(&project.0).unwrap_err();
        assert!(error.message.contains("duplicate agent name `coder`"));
    }
}
