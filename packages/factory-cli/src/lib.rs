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
    config::{load_configuration, load_configuration_snapshot, write_and_load_snapshot},
    pi::PiAgentExecutor,
    run_store::{PersistedRunStatus, RunStore},
    shell::{ShellRunnerAdapter, ShellRunnerConfig},
};
use factory_core::{
    Pipeline, StepKind,
    features::{RunError, RunJournal, RunReport, RunStatus, StepResult, run_pipeline_with_journal},
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
    if args.first().is_some_and(|arg| arg == "run") {
        return run_management_command(args);
    }
    if args.len() < 2 {
        eprintln!("Usage: factory <pipeline-name> <task> | factory run <show|resume> <run-id>");
        return EXIT_USAGE_ERROR;
    }
    run_new_pipeline(&args[0], &args[1..].join(" "))
}

fn run_new_pipeline(pipeline_name: &str, task: &str) -> u8 {
    let root = match std::env::current_dir() {
        Ok(root) => root,
        Err(error) => {
            eprintln!("Could not determine current directory: {error}");
            return EXIT_CONFIGURATION_ERROR;
        }
    };
    let source_configuration = match load_configuration(&root) {
        Ok(configuration) => configuration,
        Err(error) => {
            eprintln!("Configuration error: {error}");
            return EXIT_CONFIGURATION_ERROR;
        }
    };
    let run_dir = match create_run_dir(&root) {
        Ok(path) => path,
        Err(error) => {
            eprintln!("Could not initialize run directory: {error}");
            return EXIT_EXECUTION_ERROR;
        }
    };
    let run_id = run_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_owned();
    let mut store = match RunStore::create(
        &run_dir,
        run_id.clone(),
        pipeline_name.to_owned(),
        task.to_owned(),
    ) {
        Ok(store) => store,
        Err(error) => {
            eprintln!("Could not initialize run journal: {error}");
            return EXIT_EXECUTION_ERROR;
        }
    };
    let snapshot_path = run_dir.join("config.json");
    let configuration = match write_and_load_snapshot(&source_configuration, &snapshot_path) {
        Ok(configuration) => configuration,
        Err(error) => {
            eprintln!("Configuration snapshot error: {error}");
            return fail_run(&mut store, EXIT_CONFIGURATION_ERROR);
        }
    };
    let Some(pipeline) = configuration
        .pipelines
        .iter()
        .find(|pipeline| pipeline.name == pipeline_name)
    else {
        eprintln!("Configuration error: pipeline `{pipeline_name}` was not found in snapshot");
        return fail_run(&mut store, EXIT_CONFIGURATION_ERROR);
    };
    let memory_paths =
        match initialize_agent_memories(&run_dir, pipeline, &configuration.agents, task) {
            Ok(paths) => paths,
            Err(error) => {
                eprintln!("Could not initialize run memory: {error}");
                return fail_run(&mut store, EXIT_EXECUTION_ERROR);
            }
        };

    println!("Run ID: {run_id}");
    println!("Starting pipeline `{}`", pipeline.name);
    println!("Config snapshot: {}", snapshot_path.display());
    for path in memory_paths.values() {
        println!("Agent memory: {}", path.display());
    }
    let mut agent_runner = AgentRunnerAdapter::new(PiAgentExecutor::new(memory_paths));
    let mut shell_runner = ShellRunnerAdapter::new(shell_config(&root));
    let result = {
        let mut journal = ConsoleRunJournal::new(&mut store);
        run_pipeline_with_journal(
            pipeline,
            &configuration.agents,
            &mut agent_runner,
            &mut shell_runner,
            &[],
            &mut journal,
            MAX_STEP_EXECUTIONS,
        )
    };
    finish_pipeline_result(&mut store, result)
}

fn run_management_command(args: &[String]) -> u8 {
    let Some(action) = args.get(1).map(String::as_str) else {
        eprintln!("Usage: factory run <show|resume> <run-id> [--retry-current]");
        return EXIT_USAGE_ERROR;
    };
    let Some(run_id) = args.get(2) else {
        eprintln!("Usage: factory run <show|resume> <run-id> [--retry-current]");
        return EXIT_USAGE_ERROR;
    };
    if let Err(error) = validate_run_id(run_id) {
        eprintln!("Run error: {error}");
        return EXIT_USAGE_ERROR;
    }
    if !((action == "show" && args.len() == 3)
        || (action == "resume"
            && (args.len() == 3 || (args.len() == 4 && args[3] == "--retry-current"))))
    {
        eprintln!("Usage: factory run <show|resume> <run-id> [--retry-current]");
        return EXIT_USAGE_ERROR;
    }
    let root = match std::env::current_dir() {
        Ok(root) => root,
        Err(error) => {
            eprintln!("Could not determine current directory: {error}");
            return EXIT_CONFIGURATION_ERROR;
        }
    };
    let run_dir = root.join(".factory/runs").join(run_id);
    let mut store = match RunStore::open(&run_dir) {
        Ok(store) => store,
        Err(error) => {
            eprintln!("Could not load run `{run_id}`: {error}");
            return EXIT_CONFIGURATION_ERROR;
        }
    };
    match action {
        "show" if args.len() == 3 => match store.display_json() {
            Ok(json) => {
                println!("{json}");
                EXIT_SUCCESS
            }
            Err(error) => {
                eprintln!("Could not display run `{run_id}`: {error}");
                EXIT_EXECUTION_ERROR
            }
        },
        "resume" if args.len() == 3 || (args.len() == 4 && args[3] == "--retry-current") => {
            resume_run(&root, &run_dir, &mut store, args.len() == 4)
        }
        _ => {
            eprintln!("Usage: factory run <show|resume> <run-id> [--retry-current]");
            EXIT_USAGE_ERROR
        }
    }
}

fn resume_run(
    root: &std::path::Path,
    run_dir: &std::path::Path,
    store: &mut RunStore,
    retry_current: bool,
) -> u8 {
    if store.record().status == PersistedRunStatus::Succeeded
        || (store.record().status == PersistedRunStatus::Failed
            && !store.has_unresolved_attempt()
            && !retry_current)
    {
        eprintln!("Run `{}` is already finished", store.record().run_id);
        return EXIT_USAGE_ERROR;
    }
    if store.has_unresolved_attempt() && !retry_current {
        eprintln!(
            "Run `{}` stopped during step `{}`; inspect it with `factory run show {}` and pass `--retry-current` to retry that step",
            store.record().run_id,
            store.record().current_step.as_deref().unwrap_or("unknown"),
            store.record().run_id
        );
        return EXIT_USAGE_ERROR;
    }
    if retry_current && let Err(error) = store.mark_interrupted() {
        eprintln!("Could not update run state: {error}");
        return EXIT_EXECUTION_ERROR;
    }
    if store.record().config_snapshot != "config.json" {
        eprintln!("Run contains an unsafe config snapshot path");
        return EXIT_CONFIGURATION_ERROR;
    }
    let configuration = match load_configuration_snapshot(run_dir.join("config.json")) {
        Ok(configuration) => configuration,
        Err(error) => {
            eprintln!("Could not load saved configuration: {error}");
            return EXIT_CONFIGURATION_ERROR;
        }
    };
    let Some(pipeline) = configuration
        .pipelines
        .iter()
        .find(|pipeline| pipeline.name == store.record().pipeline_name)
    else {
        eprintln!(
            "Saved pipeline `{}` was not found",
            store.record().pipeline_name
        );
        return EXIT_CONFIGURATION_ERROR;
    };
    let memory_paths = match initialize_agent_memories(
        run_dir,
        pipeline,
        &configuration.agents,
        &store.record().task,
    ) {
        Ok(paths) => paths,
        Err(error) => {
            eprintln!("Could not restore run memory: {error}");
            return EXIT_EXECUTION_ERROR;
        }
    };
    println!("Resuming run `{}`", store.record().run_id);
    let mut agent_runner = AgentRunnerAdapter::new(PiAgentExecutor::new(memory_paths));
    let mut shell_runner = ShellRunnerAdapter::new(shell_config(root));
    let completed = store.completed_results();
    let result = {
        let mut journal = ConsoleRunJournal::new(store);
        run_pipeline_with_journal(
            pipeline,
            &configuration.agents,
            &mut agent_runner,
            &mut shell_runner,
            &completed,
            &mut journal,
            MAX_STEP_EXECUTIONS,
        )
    };
    finish_pipeline_result(store, result)
}

fn finish_pipeline_result(store: &mut RunStore, result: Result<RunReport, RunError>) -> u8 {
    match result {
        Ok(report) => {
            let status = match report.status {
                RunStatus::Succeeded => PersistedRunStatus::Succeeded,
                RunStatus::Failed => PersistedRunStatus::Failed,
            };
            if let Err(error) = store.finish(status) {
                eprintln!("Could not persist final run status: {error}");
                return EXIT_EXECUTION_ERROR;
            }
            print_report(&report)
        }
        Err(error) => {
            eprintln!("Execution error: {error}");
            if let Err(journal_error) = store.finish(PersistedRunStatus::Failed) {
                eprintln!("Could not persist failed run status: {journal_error}");
            }
            EXIT_EXECUTION_ERROR
        }
    }
}

fn fail_run(store: &mut RunStore, exit_code: u8) -> u8 {
    if let Err(error) = store.finish(PersistedRunStatus::Failed) {
        eprintln!("Could not persist failed run status: {error}");
    }
    exit_code
}

fn validate_run_id(run_id: &str) -> Result<(), String> {
    if run_id.is_empty()
        || !run_id.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '-' || character == '_'
        })
    {
        return Err("run ID may contain only ASCII letters, digits, `-` and `_`".into());
    }
    Ok(())
}

fn create_run_dir(root: &std::path::Path) -> Result<PathBuf, String> {
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
    Ok(run_dir)
}

fn initialize_agent_memories(
    run_dir: &std::path::Path,
    pipeline: &Pipeline,
    agents: &[factory_core::AgentDefinition],
    task: &str,
) -> Result<HashMap<String, PathBuf>, String> {
    let agent_by_name: HashMap<&str, &factory_core::AgentDefinition> = agents
        .iter()
        .map(|agent| (agent.name.as_str(), agent))
        .collect();
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
        if !path.exists() {
            fs::write(&path, content).map_err(|error| error.to_string())?;
        } else if !path.is_file() {
            return Err(format!("run memory path is not a file: {}", path.display()));
        }
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

struct ConsoleRunJournal<'a> {
    store: &'a mut RunStore,
}

impl<'a> ConsoleRunJournal<'a> {
    fn new(store: &'a mut RunStore) -> Self {
        Self { store }
    }
}

impl RunJournal for ConsoleRunJournal<'_> {
    fn step_started(&mut self, step_id: &str) -> Result<(), String> {
        self.store.step_started(step_id)?;
        println!("Starting step `{step_id}`");
        Ok(())
    }

    fn step_completed(&mut self, result: &StepResult) -> Result<(), String> {
        self.store.step_completed(result)?;
        println!("Completed step `{}` ({})", result.step_id, result.outcome);
        Ok(())
    }

    fn step_failed(&mut self, step_id: &str, error: &str) -> Result<(), String> {
        self.store.step_failed(step_id, error)?;
        eprintln!("Step `{step_id}` execution error: {error}");
        Ok(())
    }
}

fn print_report(report: &RunReport) -> u8 {
    for step in &report.steps {
        println!("Step `{}`: {}", step.step_id, step.outcome);
        if let Some(exit_code) = step.exit_code {
            println!("  exit code: {exit_code}");
        }
        if step.runner_attempts > 1 {
            println!("  runner attempts: {}", step.runner_attempts);
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
    use factory_core::{Step, features::RunError};

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
    fn resumes_from_saved_snapshot_without_rerunning_completed_steps() {
        use factory_adapters::{
            config::Configuration,
            run_store::{PersistedRunStatus, RunStore},
        };
        use factory_core::{
            Transition,
            features::{RunJournal, StepResult},
        };

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("factory-resume-{unique}"));
        let run_dir = root.join(".factory/runs/resume-test");
        fs::create_dir_all(&run_dir).unwrap();
        let first_marker = root.join("first-ran");
        let second_marker = root.join("second-ran");
        let pipeline = Pipeline {
            name: "resume-test".into(),
            steps: vec![
                Step {
                    id: "first".into(),
                    kind: StepKind::Shell {
                        command: "touch".into(),
                        args: vec![first_marker.to_string_lossy().into_owned()],
                    },
                    transitions: vec![Transition {
                        outcome: "success".into(),
                        target_step: "second".into(),
                    }],
                },
                Step {
                    id: "second".into(),
                    kind: StepKind::Shell {
                        command: "touch".into(),
                        args: vec![second_marker.to_string_lossy().into_owned()],
                    },
                    transitions: vec![],
                },
            ],
        };
        let configuration = Configuration {
            agents: vec![],
            pipelines: vec![pipeline],
        };
        write_and_load_snapshot(&configuration, run_dir.join("config.json")).unwrap();
        let mut store = RunStore::create(
            &run_dir,
            "resume-test".into(),
            "resume-test".into(),
            "test task".into(),
        )
        .unwrap();
        store.step_started("first").unwrap();
        store
            .step_completed(&StepResult {
                step_id: "first".into(),
                outcome: "success".into(),
                output: "already completed".into(),
                exit_code: Some(0),
                runner_attempts: 1,
                handoff: None,
            })
            .unwrap();
        store.mark_interrupted().unwrap();

        assert_eq!(resume_run(&root, &run_dir, &mut store, false), EXIT_SUCCESS);
        assert!(!first_marker.exists(), "completed shell step was rerun");
        assert!(second_marker.exists(), "next shell step did not run");
        assert_eq!(store.record().status, PersistedRunStatus::Succeeded);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_run_ids_that_could_escape_the_runs_directory() {
        assert!(validate_run_id("1794914918274172000-23366").is_ok());
        assert!(validate_run_id("../outside").is_err());
        assert!(validate_run_id("with/slash").is_err());
    }

    #[test]
    fn creates_one_markdown_memory_file_per_unique_agent_in_a_run() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("factory-memory-{unique}"));
        let agents = [agent("coder"), agent("reviewer")];
        let pipeline = Pipeline {
            name: "feature".into(),
            steps: vec![
                agent_step("build", "coder"),
                agent_step("review", "reviewer"),
                agent_step("revise", "coder"),
            ],
        };

        let run_dir = create_run_dir(&root).unwrap();
        let memories =
            initialize_agent_memories(&run_dir, &pipeline, &agents, "Add the feature").unwrap();
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
