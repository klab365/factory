//! Command-line entry point for locally running configured pipelines.

use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use factory_adapters::{
    agent::AgentRunnerAdapter,
    config::load_configuration,
    pi::PiAgentExecutor,
    shell::{ShellRunnerAdapter, ShellRunnerConfig},
};
use factory_core::{
    Pipeline, Step, StepKind,
    features::{RunReport, RunStatus, run_pipeline},
};

pub const EXIT_SUCCESS: u8 = 0;
pub const EXIT_CONFIGURATION_ERROR: u8 = 2;
pub const EXIT_EXECUTION_ERROR: u8 = 3;
pub const EXIT_REVIEW_FAILED: u8 = 4;
pub const EXIT_USAGE_ERROR: u8 = 64;
const MAX_STEP_EXECUTIONS: usize = 100;
const SHELL_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Runs the CLI for arguments after the executable name and returns its process exit code.
pub fn run_cli(args: &[String]) -> u8 {
    if args.len() < 2 {
        eprintln!("Usage: factory <pipeline-name> <task>");
        return EXIT_USAGE_ERROR;
    }
    let pipeline_name = &args[0];
    let task = args[1..].join(" ");
    let root = match std::env::current_dir() {
        Ok(root) => root,
        Err(error) => {
            eprintln!("Could not determine current directory: {error}");
            return EXIT_CONFIGURATION_ERROR;
        }
    };
    let configuration = match load_configuration(&root) {
        Ok(configuration) => configuration,
        Err(error) => {
            eprintln!("Configuration error: {error}");
            return EXIT_CONFIGURATION_ERROR;
        }
    };
    let Some(pipeline) = configuration
        .pipelines
        .iter()
        .find(|pipeline| pipeline.name == *pipeline_name)
    else {
        eprintln!("Configuration error: pipeline `{pipeline_name}` was not found");
        return EXIT_CONFIGURATION_ERROR;
    };
    let memory_paths =
        match initialize_agent_memories(&root, pipeline, &configuration.agents, &task) {
            Ok(paths) => paths,
            Err(error) => {
                eprintln!("Could not initialize run memory: {error}");
                return EXIT_EXECUTION_ERROR;
            }
        };

    println!("Starting pipeline `{}`", pipeline.name);
    for path in memory_paths.values() {
        println!("Agent memory: {}", path.display());
    }
    let mut agent_runner = AgentRunnerAdapter::new(PiAgentExecutor::new(memory_paths));
    let mut shell_runner = ShellRunnerAdapter::new(shell_config(&root));
    match run_pipeline(
        pipeline,
        &configuration.agents,
        &mut agent_runner,
        &mut shell_runner,
        MAX_STEP_EXECUTIONS,
    ) {
        Ok(report) => print_report(&report),
        Err(error) => {
            eprintln!("Execution error: {error}");
            EXIT_EXECUTION_ERROR
        }
    }
}

fn initialize_agent_memories(
    root: &std::path::Path,
    pipeline: &Pipeline,
    agents: &[factory_core::AgentDefinition],
    task: &str,
) -> Result<HashMap<String, PathBuf>, String> {
    let agent_by_name: HashMap<&str, &factory_core::AgentDefinition> = agents
        .iter()
        .map(|agent| (agent.name.as_str(), agent))
        .collect();
    let run_id = format!(
        "{}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_nanos(),
        std::process::id()
    );
    let run_dir = root.join(".factory/runs").join(run_id);
    fs::create_dir_all(&run_dir).map_err(|error| error.to_string())?;

    let mut seen = HashSet::new();
    let mut memories = HashMap::new();
    for step in &pipeline.steps {
        let StepKind::Agent { agent: agent_name } = &step.kind else {
            continue;
        };
        if !seen.insert(agent_name.as_str()) {
            continue;
        }
        let agent = agent_by_name
            .get(agent_name.as_str())
            .ok_or_else(|| format!("pipeline references missing agent `{agent_name}`"))?;
        let index = memories.len();
        let slug: String = agent
            .name
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                    character
                } else {
                    '_'
                }
            })
            .collect();
        let path = run_dir.join(format!("{index:02}-{slug}.md"));
        let content = format!(
            "# Run memory: {}\n\nPipeline: {}\n\n## Auftrag\n\n{}\n\n## Verlauf\n\n<!-- Der Agent ergänzt hier Ergebnisse, Entscheidungen und offene Punkte. -->\n",
            agent.name, pipeline.name, task
        );
        fs::write(&path, content).map_err(|error| error.to_string())?;
        memories.insert(agent.name.clone(), path);
    }
    Ok(memories)
}

fn shell_config(root: &std::path::Path) -> ShellRunnerConfig {
    let mut config = ShellRunnerConfig::new(root, SHELL_TIMEOUT);
    // Commands receive only these explicitly selected variables, not the full parent environment.
    for key in ["PATH", "HOME", "TMPDIR", "TMP", "TEMP", "SYSTEMROOT"] {
        if let Some(value) = std::env::var_os(key) {
            config.environment.insert(OsString::from(key), value);
        }
    }
    config
}

fn print_report(report: &RunReport) -> u8 {
    for step in &report.steps {
        println!("Step `{}`: {}", step.step_id, step.outcome);
        if let Some(exit_code) = step.exit_code {
            println!("  exit code: {exit_code}");
        }
        if !step.output.is_empty() {
            println!("  {}", step.output.replace('\n', "\n  "));
        }
    }
    match report.status {
        RunStatus::Succeeded => {
            println!("Pipeline `{}` succeeded", report.pipeline_name);
            EXIT_SUCCESS
        }
        RunStatus::Failed => {
            println!(
                "Pipeline `{}` failed (review or step outcome)",
                report.pipeline_name
            );
            EXIT_REVIEW_FAILED
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use factory_core::features::RunError;

    #[test]
    fn distinguishes_terminal_failure_from_execution_errors() {
        let report = RunReport {
            pipeline_name: "review".into(),
            status: RunStatus::Failed,
            steps: vec![],
        };
        assert_eq!(print_report(&report), EXIT_REVIEW_FAILED);
        assert_eq!(
            execution_exit_code(Err(RunError::RunnerFailed {
                step_id: "test".into(),
                message: "failed".into(),
            })),
            EXIT_EXECUTION_ERROR
        );
    }

    #[test]
    fn rejects_invalid_argument_counts() {
        assert_eq!(run_cli(&[]), EXIT_USAGE_ERROR);
        assert_eq!(run_cli(&["feature".into()]), EXIT_USAGE_ERROR);
    }

    #[test]
    fn creates_one_markdown_memory_file_per_unique_agent_in_a_run() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("factory-cli-memory-{unique}"));
        let agents = [agent("coder"), agent("reviewer")];
        let pipeline = Pipeline {
            name: "feature".into(),
            steps: vec![
                agent_step("build", "coder"),
                agent_step("review", "reviewer"),
                agent_step("revise", "coder"),
            ],
        };

        let memories =
            initialize_agent_memories(&root, &pipeline, &agents, "Add the feature").unwrap();
        assert_eq!(memories.len(), 2);
        for path in memories.values() {
            assert!(
                fs::read_to_string(path)
                    .unwrap()
                    .contains("Add the feature")
            );
        }
        let run_dir = memories.values().next().unwrap().parent().unwrap();
        assert_eq!(fs::read_dir(run_dir).unwrap().count(), 2);
        fs::remove_dir_all(root).unwrap();
    }

    fn agent(name: &str) -> factory_core::AgentDefinition {
        factory_core::AgentDefinition {
            name: name.into(),
            harness: "pi".into(),
            model: factory_core::ModelRef::parse("test/model").unwrap(),
            thinking_level: None,
            prompt: "prompt".into(),
        }
    }

    fn agent_step(id: &str, agent: &str) -> Step {
        Step {
            id: id.into(),
            kind: StepKind::Agent {
                agent: agent.into(),
            },
            transitions: vec![],
        }
    }

    fn execution_exit_code(result: Result<RunReport, RunError>) -> u8 {
        match result {
            Ok(report) => print_report(&report),
            Err(error) => {
                eprintln!("Execution error: {error}");
                EXIT_EXECUTION_ERROR
            }
        }
    }
}
