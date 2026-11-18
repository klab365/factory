//! TOML and YAML configuration loading and validation for factory-core domain types.

use std::{
    collections::HashSet,
    error::Error,
    fmt,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use factory_core::{
    AgentContextInput, AgentDefinition, HumanOption, ModelRef, Pipeline, PipelineInput, Step,
    StepKind, ThinkingLevel, Transition,
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

/// Loads and validates all agent and pipeline TOML or YAML files beneath `.factory/`.
pub fn load_configuration(root: impl AsRef<Path>) -> Result<Configuration, ConfigError> {
    let root = root.as_ref();
    let agent_dir = root.join(".factory/agents");
    let pipeline_dir = root.join(".factory/pipelines");
    let agent_paths = configuration_files(&agent_dir)?;
    let pipeline_paths = configuration_files(&pipeline_dir)?;

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

fn configuration_files(dir: &Path) -> Result<Vec<PathBuf>, ConfigError> {
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
            && path.extension().is_some_and(|extension| {
                extension == "toml" || extension == "yml" || extension == "yaml"
            })
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
    let extension = path.extension().and_then(|value| value.to_str());
    match extension {
        Some("yml" | "yaml") => serde_yaml::from_str(&source).map_err(|error| ConfigError {
            path: path.to_owned(),
            message: format!("invalid YAML: {error}"),
        }),
        Some("toml") => toml::from_str(&source).map_err(|error| ConfigError {
            path: path.to_owned(),
            message: format!("invalid TOML: {error}"),
        }),
        _ => Err(invalid(path, "unsupported configuration file extension")),
    }
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
                    StepKind::Agent { agent, context } => StepDto::Agent {
                        id: step.id.clone(),
                        agent: agent.clone(),
                        context: context
                            .iter()
                            .map(|input| {
                                (
                                    input.name.clone(),
                                    AgentContextDto {
                                        from: input.source.clone(),
                                        optional: input.optional,
                                    },
                                )
                            })
                            .collect(),
                        transitions,
                    },
                    StepKind::Shell { command, args } => StepDto::Shell {
                        id: step.id.clone(),
                        command: command.clone(),
                        args: args.clone(),
                        transitions,
                    },
                    StepKind::Human {
                        prompt,
                        options,
                        approval_keyword,
                    } => StepDto::Human {
                        id: step.id.clone(),
                        prompt: prompt.clone(),
                        approval_keyword: approval_keyword.clone(),
                        options: options
                            .iter()
                            .map(|option| HumanOptionDto {
                                id: option.id.clone(),
                                label: option.label.clone(),
                            })
                            .collect(),
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
        context: std::collections::BTreeMap<String, AgentContextDto>,
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
    Human {
        id: String,
        prompt: String,
        #[serde(default)]
        approval_keyword: Option<String>,
        #[serde(default)]
        options: Vec<HumanOptionDto>,
        #[serde(default)]
        transitions: Vec<TransitionDto>,
    },
}

#[derive(Serialize, Deserialize)]
struct AgentContextDto {
    from: String,
    #[serde(default)]
    optional: bool,
}

fn is_valid_context_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn validate_context_source(
    source: &str,
    inputs: &HashSet<String>,
    step_ids: &HashSet<String>,
) -> Result<(), String> {
    let parts: Vec<_> = source.split('.').collect();
    match parts.as_slice() {
        ["run", "task"] => Ok(()),
        ["inputs", input] if inputs.contains(*input) => Ok(()),
        ["steps", step_id, "output"] if step_ids.contains(*step_id) => Ok(()),
        ["steps", step_id, "handoff"] if step_ids.contains(*step_id) => Ok(()),
        ["steps", step_id, "handoff", field]
            if step_ids.contains(*step_id) && is_valid_context_name(field) =>
        {
            Ok(())
        }
        ["inputs", input] => Err(format!("references undeclared pipeline input `{input}`")),
        ["steps", step_id, ..] if !step_ids.contains(*step_id) => {
            Err(format!("references unknown step `{step_id}`"))
        }
        _ => Err(format!(
            "invalid source `{source}`; expected `run.task`, `inputs.<key>`, or `steps.<id>.output|handoff[.<field>]`"
        )),
    }
}

#[derive(Serialize, Deserialize)]
struct HumanOptionDto {
    id: String,
    label: String,
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
    if inputs.is_empty() {
        return Err(invalid(
            path,
            format!("pipeline `{}` must declare at least one input", dto.name),
        ));
    }
    if dto.steps.is_empty() {
        return Err(invalid(
            path,
            format!("pipeline `{}` must contain at least one step", dto.name),
        ));
    }

    let agent_names: HashSet<&str> = agents.iter().map(|agent| agent.name.as_str()).collect();
    let declared_step_ids: HashSet<String> = dto
        .steps
        .iter()
        .map(|step| match step {
            StepDto::Agent { id, .. } | StepDto::Shell { id, .. } | StepDto::Human { id, .. } => {
                id.clone()
            }
        })
        .collect();
    let mut step_ids = HashSet::new();
    let mut steps = Vec::with_capacity(dto.steps.len());
    for step in dto.steps {
        let (id, kind, transitions) = match step {
            StepDto::Agent {
                id,
                agent,
                context,
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
                let context = context
                    .into_iter()
                    .map(|(name, input)| {
                        if !is_valid_context_name(&name) {
                            return Err(invalid(
                                path,
                                format!(
                                    "pipeline `{}` step `{id}` has invalid context name `{name}`",
                                    dto.name
                                ),
                            ));
                        }
                        if let Err(message) =
                            validate_context_source(&input.from, &input_names, &declared_step_ids)
                        {
                            return Err(invalid(
                                path,
                                format!(
                                    "pipeline `{}` step `{id}` context `{name}`: {message}",
                                    dto.name
                                ),
                            ));
                        }
                        Ok(AgentContextInput {
                            name,
                            source: input.from,
                            optional: input.optional,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                (id, StepKind::Agent { agent, context }, transitions)
            }
            StepDto::Human {
                id,
                prompt,
                approval_keyword,
                options,
                transitions,
            } => {
                if prompt.trim().is_empty() {
                    return Err(invalid(
                        path,
                        format!(
                            "pipeline `{}` step `{id}` field `prompt` must not be empty",
                            dto.name
                        ),
                    ));
                }
                if let Some(keyword) = &approval_keyword {
                    if keyword.trim().is_empty() || !options.is_empty() {
                        return Err(invalid(
                            path,
                            format!(
                                "pipeline `{}` step `{id}` approval_keyword must be non-empty and cannot be combined with options",
                                dto.name
                            ),
                        ));
                    }
                }
                let mut option_ids = HashSet::new();
                let options = options.into_iter().map(|option| {
                    if option.id.trim().is_empty()
                        || option.id.trim() != option.id
                        || option.label.trim().is_empty()
                        || !option_ids.insert(option.id.clone())
                    {
                        return Err(invalid(
                            path,
                            format!(
                                "pipeline `{}` step `{id}` has an empty, whitespace-padded, or duplicate human option ID/label",
                                dto.name
                            ),
                        ));
                    }
                    Ok(HumanOption { id: option.id, label: option.label })
                }).collect::<Result<Vec<_>, _>>()?;
                (
                    id,
                    StepKind::Human {
                        prompt,
                        options,
                        approval_keyword,
                    },
                    transitions,
                )
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
    const YAML_AGENT: &str = "name: coder\nharness: pi\nmodel: openai-codex/gpt-6-luna\nthinking_level: high\nprompt: Implement the task.\n";

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
                agent: "coder".into(),
                context: vec![],
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
        let agent_path = project.file(".factory/agents/coder.yml", YAML_AGENT);
        project.file(
            ".factory/pipelines/feature.yaml",
            "name: feature\ninputs:\n  - key: task\n    description: Task to implement\nsteps:\n  - id: build\n    type: agent\n    agent: coder\n    context:\n      task:\n        from: run.task\n      request:\n        from: inputs.task\n",
        );
        let original = load_configuration(&project.0).unwrap();
        assert_eq!(
            original.pipelines[0].steps[0].kind,
            StepKind::Agent {
                agent: "coder".into(),
                context: vec![
                    AgentContextInput {
                        name: "request".into(),
                        source: "inputs.task".into(),
                        optional: false,
                    },
                    AgentContextInput {
                        name: "task".into(),
                        source: "run.task".into(),
                        optional: false,
                    },
                ],
            }
        );
        let run_dir = project.0.join(".factory/runs/run-1");
        fs::create_dir_all(&run_dir).unwrap();
        let snapshot_path = run_dir.join("config.json");

        let snapshot_config = write_and_load_snapshot(&original, &snapshot_path).unwrap();
        assert_eq!(snapshot_config, original);
        let json = fs::read_to_string(&snapshot_path).unwrap();
        assert!(json.contains("\"version\": 1"));

        fs::write(
            agent_path,
            YAML_AGENT.replace("Implement the task.", "Changed after run start."),
        )
        .unwrap();
        assert_eq!(
            load_configuration_snapshot(&snapshot_path).unwrap(),
            original
        );
    }

    #[test]
    fn loads_repository_feature_pipeline_context_bindings() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let configuration = load_configuration(root).unwrap();
        let feature = configuration
            .pipelines
            .iter()
            .find(|pipeline| pipeline.name == "feature")
            .unwrap();
        let StepKind::Agent { context, .. } = &feature
            .steps
            .iter()
            .find(|step| step.id == "build")
            .unwrap()
            .kind
        else {
            panic!("build step should be an agent");
        };
        assert!(context.iter().any(|input| {
            input.name == "human_feedback"
                && input.source == "steps.approval.handoff.answer"
                && input.optional
        }));
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
    fn migrated_feature_pipeline_passes_github_json_fields_as_one_argument() {
        let project = TempProject::new();
        project.file(".factory/agents/coder.yaml", YAML_AGENT);
        project.file(
            ".factory/agents/planner.yaml",
            &YAML_AGENT.replace("name: coder", "name: planner"),
        );
        project.file(
            ".factory/agents/reviewer.yaml",
            &YAML_AGENT.replace("name: coder", "name: reviewer"),
        );
        project.file(
            ".factory/agents/publisher.yaml",
            &YAML_AGENT.replace("name: coder", "name: publisher"),
        );
        project.file(
            ".factory/pipelines/feature.yaml",
            include_str!("../../../.factory/pipelines/feature.yaml"),
        );

        let config = load_configuration(&project.0).unwrap();
        let StepKind::Shell { args, .. } = &config.pipelines[0].steps[0].kind else {
            panic!("issue-intake must be a shell step");
        };
        let json_flag = args.iter().position(|arg| arg == "--json").unwrap();
        assert_eq!(&args[json_flag + 1..], &["url,number,title,body,labels"]);
        assert!(
            config.pipelines[0]
                .steps
                .iter()
                .any(|step| step.id == "publish")
        );
    }

    #[test]
    fn loads_both_yaml_extensions_and_validates_core_types() {
        let project = TempProject::new();
        project.file(".factory/agents/coder.yml", YAML_AGENT);
        project.file(
            ".factory/pipelines/feature.yaml",
            "name: feature\ninputs:\n  - key: target\n    description: The target value\nsteps:\n  - id: build\n    type: agent\n    agent: coder\n    transitions:\n      - outcome: success\n        target_step: test\n  - id: test\n    type: shell\n    command: cargo\n    args: [test]\n",
        );
        let config = load_configuration(&project.0).unwrap();
        assert_eq!(config.agents[0].name, "coder");
        assert_eq!(config.pipelines[0].inputs[0].key, "target");
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
    fn reports_yaml_syntax_errors_with_the_file_path() {
        let project = TempProject::new();
        let path = project.file(".factory/agents/broken.yaml", "name: [");
        let error = load_configuration(&project.0).unwrap_err();
        assert!(error.to_string().contains(path.to_str().unwrap()));
        assert!(error.message.contains("invalid YAML"));
    }

    #[test]
    fn rejects_pipelines_without_inputs() {
        let project = TempProject::new();
        project.file(".factory/agents/coder.yaml", YAML_AGENT);
        project.file(
            ".factory/pipelines/feature.yaml",
            "name: feature\nsteps:\n  - id: build\n    type: agent\n    agent: coder\n",
        );
        let error = load_configuration(&project.0).unwrap_err();
        assert!(error.message.contains("must declare at least one input"));
    }

    #[test]
    fn reports_yaml_semantic_reference_errors() {
        let project = TempProject::new();
        project.file(".factory/agents/coder.yml", YAML_AGENT);
        project.file(
            ".factory/pipelines/feature.yaml",
            "name: feature\ninputs:\n  - key: task\n    description: Task to implement\nsteps:\n  - id: build\n    type: agent\n    agent: missing\n",
        );
        let error = load_configuration(&project.0).unwrap_err();
        assert!(error.message.contains("unknown agent `missing`"));
    }

    #[test]
    fn reports_semantic_reference_errors_with_pipeline_and_step() {
        let project = TempProject::new();
        project.file(".factory/agents/coder.toml", AGENT);
        project.file(
            ".factory/pipelines/feature.toml",
            "name = 'feature'\n\n[[inputs]]\nkey = 'task'\ndescription = 'Task to implement'\n\n[[steps]]\nid = 'build'\ntype = 'agent'\nagent = 'missing'\n",
        );
        let error = load_configuration(&project.0).unwrap_err();
        assert!(error.message.contains("pipeline `feature` step `build`"));
        assert!(error.message.contains("unknown agent `missing`"));
    }

    #[test]
    fn rejects_human_option_ids_with_surrounding_whitespace_and_normalization_collisions() {
        for ids in [
            [" approve", "reject"],
            ["approve ", "reject"],
            ["approve", " approve "],
        ] {
            let project = TempProject::new();
            project.file(".factory/agents/coder.toml", AGENT);
            let options = ids
                .iter()
                .map(|id| format!("{{ id = '{id}', label = 'Option' }}"))
                .collect::<Vec<_>>()
                .join(", ");
            project.file(
                ".factory/pipelines/feature.toml",
                &format!(
                    "name = 'feature'\n[[inputs]]\nkey = 'task'\ndescription = 'Task'\n[[steps]]\nid = 'ask'\ntype = 'human'\nprompt = 'Choose'\noptions = [{options}]\n"
                ),
            );
            let error = load_configuration(&project.0).unwrap_err();
            assert!(error.message.contains("whitespace-padded"), "{error}");
        }
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
