//! TOML configuration loading and validation for factory-core domain types.

use std::{
    collections::HashSet,
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
};

use factory_core::{
    AgentDefinition, ModelRef, Pipeline, Step, StepKind, ThinkingLevel, Transition,
};
use serde::Deserialize;

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

#[derive(Deserialize)]
struct AgentDto {
    name: String,
    harness: String,
    model: String,
    thinking_level: String,
    prompt: String,
}

fn validate_agent(dto: AgentDto, path: &Path) -> Result<AgentDefinition, ConfigError> {
    if dto.name.trim().is_empty() {
        return Err(invalid(path, "field `name` must not be empty"));
    }
    if dto.harness.trim().is_empty() {
        return Err(invalid(path, "field `harness` must not be empty"));
    }
    if dto.prompt.trim().is_empty() {
        return Err(invalid(
            path,
            format!("agent `{}` field `prompt` must not be empty", dto.name),
        ));
    }
    let model = ModelRef::parse(&dto.model)
        .map_err(|error| invalid(path, format!("agent `{}` field `model`: {error}", dto.name)))?;
    if dto.model.chars().any(char::is_whitespace) {
        return Err(invalid(
            path,
            format!(
                "agent `{}` field `model` must not contain whitespace",
                dto.name
            ),
        ));
    }
    let thinking_level = parse_thinking_level(&dto.thinking_level).ok_or_else(|| {
        invalid(
            path,
            format!(
                "agent `{}` has invalid field `thinking_level`: `{}`",
                dto.name, dto.thinking_level
            ),
        )
    })?;

    Ok(AgentDefinition {
        name: dto.name,
        harness: dto.harness,
        model,
        thinking_level: Some(thinking_level),
        prompt: dto.prompt,
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

#[derive(Deserialize)]
struct PipelineDto {
    name: String,
    steps: Vec<StepDto>,
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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
        time::{SystemTime, UNIX_EPOCH},
    };

    struct TempProject(PathBuf);

    impl TempProject {
        fn new() -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!("factory-adapters-config-{unique}"));
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
        project.file(".factory/pipelines/feature.toml", "name = 'feature'\n\n[[steps]]\nid = 'build'\ntype = 'agent'\nagent = 'coder'\n\n[[steps.transitions]]\noutcome = 'success'\ntarget_step = 'test'\n\n[[steps]]\nid = 'test'\ntype = 'shell'\ncommand = 'cargo'\nargs = ['test']\n");

        let config = load_configuration(&project.0).unwrap();
        assert_eq!(config.agents[0].name, "coder");
        assert_eq!(config.agents[0].thinking_level, Some(ThinkingLevel::High));
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
