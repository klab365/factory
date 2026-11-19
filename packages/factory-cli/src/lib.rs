//! Command-line entry point for locally running configured pipelines.

use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    fs,
    io::{self, IsTerminal, Write},
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
    if args.first().is_some_and(|arg| arg == "validate") {
        return validate_configuration_command(args);
    }
    if args.first().is_some_and(|arg| arg == "plan") {
        return plan_command(args);
    }
    let pipeline_args = if args.first().is_some_and(|arg| arg == "run") {
        if matches!(
            args.get(1).map(String::as_str),
            Some("show" | "resume" | "cancel")
        ) {
            return run_management_command(args);
        }
        if args.len() < 2 {
            eprintln!("Usage: factory run <pipeline-name> [--input name=value]... [task]");
            return EXIT_USAGE_ERROR;
        }
        &args[1..]
    } else {
        args
    };
    if pipeline_args.is_empty() {
        eprintln!("Usage: factory <pipeline-name> [--input name=value]... [task]");
        return EXIT_USAGE_ERROR;
    }
    let pipeline_name = &pipeline_args[0];
    let mut task_parts = Vec::new();
    let mut inputs = HashMap::new();
    let mut index = 1;
    while index < pipeline_args.len() {
        if pipeline_args[index] == "--input" {
            let Some(pair) = pipeline_args.get(index + 1) else {
                eprintln!("Usage error: `--input` requires name=value");
                return EXIT_USAGE_ERROR;
            };
            let Some((name, value)) = pair.split_once('=') else {
                eprintln!("Usage error: input must use name=value");
                return EXIT_USAGE_ERROR;
            };
            if name.is_empty() || inputs.insert(name.to_owned(), value.to_owned()).is_some() {
                eprintln!("Usage error: input names must be non-empty and unique");
                return EXIT_USAGE_ERROR;
            }
            index += 2;
        } else {
            task_parts.push(pipeline_args[index].as_str());
            index += 1;
        }
    }
    run_new_pipeline(pipeline_name, &task_parts.join(" "), inputs)
}

fn validate_configuration_command(args: &[String]) -> u8 {
    if args.len() != 1 {
        eprintln!("Usage: factory validate");
        return EXIT_USAGE_ERROR;
    }
    let root = match std::env::current_dir() {
        Ok(root) => root,
        Err(error) => {
            eprintln!("Could not determine current directory: {error}");
            return EXIT_CONFIGURATION_ERROR;
        }
    };
    match load_configuration(root) {
        Ok(configuration) => {
            println!(
                "Configuration valid ({} agents, {} pipelines)",
                configuration.agents.len(),
                configuration.pipelines.len()
            );
            EXIT_SUCCESS
        }
        Err(error) => {
            eprintln!("Configuration error: {error}");
            EXIT_CONFIGURATION_ERROR
        }
    }
}

fn plan_command(args: &[String]) -> u8 {
    if args.len() < 2 {
        eprintln!("Usage: factory plan <pipeline-name> [--input name=value]...");
        return EXIT_USAGE_ERROR;
    }
    let mut inputs = HashMap::new();
    let mut index = 2;
    while index < args.len() {
        if args[index] != "--input" || index + 1 >= args.len() {
            eprintln!("Usage error: `--input` requires name=value");
            return EXIT_USAGE_ERROR;
        }
        let Some((name, value)) = args[index + 1].split_once('=') else {
            eprintln!("Usage error: input must use name=value");
            return EXIT_USAGE_ERROR;
        };
        if name.is_empty() || inputs.insert(name.to_owned(), value.to_owned()).is_some() {
            eprintln!("Usage error: input names must be non-empty and unique");
            return EXIT_USAGE_ERROR;
        }
        index += 2;
    }
    let root = match std::env::current_dir() {
        Ok(root) => root,
        Err(error) => {
            eprintln!("Could not determine current directory: {error}");
            return EXIT_CONFIGURATION_ERROR;
        }
    };
    let configuration = match load_configuration(root) {
        Ok(configuration) => configuration,
        Err(error) => {
            eprintln!("Configuration error: {error}");
            return EXIT_CONFIGURATION_ERROR;
        }
    };
    let Some(pipeline) = configuration
        .pipelines
        .iter()
        .find(|pipeline| pipeline.name == args[1])
    else {
        eprintln!("Configuration error: pipeline `{}` was not found", args[1]);
        return EXIT_CONFIGURATION_ERROR;
    };
    if let Err(error) = validate_pipeline_inputs(pipeline, &inputs) {
        eprintln!("Input error: {error}");
        return EXIT_USAGE_ERROR;
    }
    println!(
        "Plan for pipeline `{}` (configured control flow; outcomes are not simulated)",
        pipeline.name
    );
    println!("Inputs:");
    for input in &pipeline.inputs {
        println!("  {} = {}", input.key, inputs[&input.key]);
    }
    println!("Steps:");
    for step in &pipeline.steps {
        let kind = match &step.kind {
            StepKind::Agent { .. } => "agent",
            StepKind::Shell { .. } => "shell",
            StepKind::Human { .. } => "human",
        };
        println!("  {} ({kind})", step.id);
        for transition in &step.transitions {
            println!(
                "    outcome `{}` -> `{}`",
                transition.outcome, transition.target_step
            );
        }
        if step.transitions.is_empty() {
            println!("    (terminal)");
        }
    }
    EXIT_SUCCESS
}

fn run_new_pipeline(pipeline_name: &str, task: &str, inputs: HashMap<String, String>) -> u8 {
    let root = match std::env::current_dir() {
        Ok(root) => root,
        Err(error) => {
            eprintln!("Could not determine current directory: {error}");
            return EXIT_CONFIGURATION_ERROR;
        }
    };
    run_new_pipeline_from_root(&root, pipeline_name, task, inputs)
}

fn run_new_pipeline_from_root(
    root: &std::path::Path,
    pipeline_name: &str,
    task: &str,
    inputs: HashMap<String, String>,
) -> u8 {
    let source_configuration = match load_configuration(root) {
        Ok(configuration) => configuration,
        Err(error) => {
            eprintln!("Configuration error: {error}");
            return EXIT_CONFIGURATION_ERROR;
        }
    };
    let Some(source_pipeline) = source_configuration
        .pipelines
        .iter()
        .find(|pipeline| pipeline.name == pipeline_name)
    else {
        eprintln!("Configuration error: pipeline `{pipeline_name}` was not found");
        return EXIT_CONFIGURATION_ERROR;
    };
    let mut inputs = inputs;
    for input in &source_pipeline.inputs {
        if !inputs.contains_key(&input.key) {
            match prompt_value(&format!("{} ({}): ", input.key, input.description)) {
                Ok(value) => {
                    inputs.insert(input.key.clone(), value);
                }
                Err(error) => {
                    eprintln!("Input error: {error}");
                    return EXIT_USAGE_ERROR;
                }
            }
        }
    }
    let task = task_for_run(task, &inputs);
    if let Err(error) = validate_pipeline_inputs(source_pipeline, &inputs) {
        eprintln!("Input error: {error}");
        return EXIT_USAGE_ERROR;
    }
    let run_dir = match create_run_dir(root) {
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
        task.clone(),
        inputs.clone(),
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
    if let Err(error) = validate_pipeline_inputs(pipeline, &inputs) {
        eprintln!("Input error: {error}");
        return fail_run(&mut store, EXIT_USAGE_ERROR);
    }
    let memory_paths =
        match initialize_agent_memories(&run_dir, pipeline, &configuration.agents, &task) {
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
    let mut shell_runner = ShellRunnerAdapter::new(shell_config(root, inputs));
    execute_pipeline_and_finish(
        &mut store,
        pipeline,
        &configuration.agents,
        &mut agent_runner,
        &mut shell_runner,
        &[],
    )
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
    if !((action == "show" && (args.len() == 3 || (args.len() == 4 && args[3] == "--text")))
        || (action == "cancel" && args.len() == 3)
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
        "show" if args.len() == 3 || (args.len() == 4 && args[3] == "--text") => {
            let record = store.record();
            if args.len() == 3 {
                match serde_json::to_string_pretty(record) {
                    Ok(json) => println!("{json}"),
                    Err(error) => {
                        eprintln!("Could not serialize run `{run_id}`: {error}");
                        return EXIT_CONFIGURATION_ERROR;
                    }
                }
                return EXIT_SUCCESS;
            }
            let end = record.finished_at_ms.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(record.updated_at_ms, |duration| duration.as_millis())
            });
            let completed_steps = record
                .steps
                .iter()
                .filter(|step| {
                    step.status == factory_adapters::run_store::StepAttemptStatus::Completed
                })
                .map(|step| step.step_id.as_str())
                .collect::<std::collections::HashSet<_>>()
                .len();
            println!(
                "Run: {}\nPipeline: {}\nStatus: {}\nCurrent/last step: {}\nCompleted distinct steps: {}\nStep attempts: {}\nDuration: {} ms",
                record.run_id,
                record.pipeline_name,
                format!("{:?}", record.status).to_lowercase(),
                record.current_step.as_deref().unwrap_or("(none)"),
                completed_steps,
                record.steps.len(),
                end.saturating_sub(record.started_at_ms)
            );
            if let Some(pending) = &record.pending_human {
                println!(
                    "Waiting for input at {}: {}",
                    pending.step_id, pending.prompt
                );
            }
            for attempt in &record.steps {
                println!(
                    "Step {} attempt {}: {:?}{}",
                    attempt.step_id,
                    attempt.attempt,
                    attempt.status,
                    attempt
                        .error
                        .as_ref()
                        .map_or(String::new(), |error| format!(" — {error}"))
                );
            }
            EXIT_SUCCESS
        }
        "cancel" if args.len() == 3 => match store.cancel() {
            Ok(()) => {
                if let Some(step) = store.record().steps.iter().rev().find(|step| {
                    step.status == factory_adapters::run_store::StepAttemptStatus::Cancelled
                }) && let Err(error) = append_pipeline_history(
                    store.run_dir(),
                    &step.step_id,
                    step.attempt,
                    "cancelled",
                ) {
                    eprintln!("Could not append pipeline history: {error}");
                    return EXIT_EXECUTION_ERROR;
                }
                println!("Run `{run_id}` cancelled");
                EXIT_SUCCESS
            }
            Err(error) => {
                eprintln!("Could not cancel run `{run_id}`: {error}");
                EXIT_USAGE_ERROR
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
    if store.record().status == PersistedRunStatus::Cancelled {
        eprintln!(
            "Run `{}` is cancelled and cannot be resumed",
            store.record().run_id
        );
        return EXIT_USAGE_ERROR;
    }
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
    if retry_current
        && store.record().status != PersistedRunStatus::WaitingForInput
        && let Err(error) = store.mark_interrupted()
    {
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
    if let Err(error) = validate_pipeline_inputs(pipeline, &store.record().inputs) {
        eprintln!("Saved run input error: {error}");
        return EXIT_CONFIGURATION_ERROR;
    }
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
    let mut shell_runner =
        ShellRunnerAdapter::new(shell_config(root, store.record().inputs.clone()));
    let mut completed = store.completed_results();
    if store.record().status == PersistedRunStatus::WaitingForInput {
        let answer = read_resume_answer(store);
        match answer.and_then(|answer| store.answer_pending_human(&answer)) {
            Ok(result) => {
                let attempt = store.record().steps.last().map_or(1, |step| step.attempt);
                if let Err(error) = append_pipeline_history(
                    store.run_dir(),
                    &result.step_id,
                    attempt,
                    &result.outcome,
                ) {
                    eprintln!("Could not append pipeline history: {error}");
                    return EXIT_EXECUTION_ERROR;
                }
                completed.push(result);
            }
            Err(error) => {
                eprintln!("Human answer error: {error}");
                return EXIT_USAGE_ERROR;
            }
        }
    }
    execute_pipeline_and_finish(
        store,
        pipeline,
        &configuration.agents,
        &mut agent_runner,
        &mut shell_runner,
        &completed,
    )
}

fn execute_pipeline_and_finish<
    A: factory_core::features::AgentRunner,
    S: factory_core::features::ShellRunner,
>(
    store: &mut RunStore,
    pipeline: &Pipeline,
    agents: &[factory_core::AgentDefinition],
    agent_runner: &mut A,
    shell_runner: &mut S,
    completed: &[StepResult],
) -> u8 {
    let result = {
        let mut journal = ConsoleRunJournal::new(store);
        run_pipeline_with_journal(
            pipeline,
            agents,
            agent_runner,
            shell_runner,
            completed,
            &mut journal,
            MAX_STEP_EXECUTIONS,
        )
    };
    finish_pipeline_result(store, result, pipeline)
}

fn finish_pipeline_result(
    store: &mut RunStore,
    result: Result<RunReport, RunError>,
    pipeline: &Pipeline,
) -> u8 {
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
            print_report(&report, pipeline)
        }
        Err(error) => {
            if store.is_cancelled() {
                eprintln!("Run `{}` was cancelled", store.record().run_id);
                return EXIT_SUCCESS;
            }
            if store.record().status == PersistedRunStatus::WaitingForInput {
                eprintln!("{}", error);
                return EXIT_SUCCESS;
            }
            eprintln!("Execution error: {error}");
            if let Err(journal_error) = store.finish(PersistedRunStatus::Failed) {
                eprintln!("Could not persist failed run status: {journal_error}");
            }
            EXIT_EXECUTION_ERROR
        }
    }
}

fn read_resume_answer(store: &RunStore) -> Result<String, String> {
    let pending = store
        .record()
        .pending_human
        .as_ref()
        .ok_or("pending question is missing")?;
    println!("{}", pending.prompt);
    for option in &pending.options {
        println!("  {}: {}", option.id, option.label);
    }
    if !io::stdin().is_terminal() {
        return Err("Human answer required; rerun resume interactively".into());
    }
    eprint!("Answer: ");
    io::stderr().flush().map_err(|e| e.to_string())?;
    let answer = read_interactive_line_or_cancel(store)?;
    Ok(answer.trim().to_owned())
}

fn read_interactive_line_or_cancel(store: &RunStore) -> Result<String, String> {
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let result = io::stdin()
            .read_line(&mut line)
            .map(|bytes| if bytes == 0 { None } else { Some(line) })
            .map_err(|error| error.to_string());
        let _ = sender.send(result);
    });
    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(Ok(Some(line))) => return Ok(line),
            Ok(Ok(None)) => return Err("stdin is closed".into()),
            Ok(Err(error)) => return Err(format!("could not read human answer: {error}")),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if store.is_cancelled() {
                    return Err("run was cancelled".into());
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err("stdin reader stopped unexpectedly".into());
            }
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
        let StepKind::Agent {
            agent: agent_name, ..
        } = &step.kind
        else {
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
            "# Run memory: {}\n\nPipeline: {}\n\n## Auftrag\n\n{}\n\n## Verlauf\n\n<!-- Chronologische Einträge append-only als `### <ID>. Verlauf: Step ... (Versuch ...) — Outcome ...; Handoff <ID>` ergänzen. -->\n",
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

fn append_pipeline_history(
    run_dir: &std::path::Path,
    step_id: &str,
    attempt: u32,
    outcome: &str,
) -> Result<(), String> {
    let mut files = fs::read_dir(run_dir)
        .map_err(|error| format!("could not read run directory for history: {error}"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension().is_some_and(|extension| extension == "md")
                && path
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.split_once('-').is_some_and(|(prefix, _)| {
                            !prefix.is_empty() && prefix.bytes().all(|byte| byte.is_ascii_digit())
                        })
                    })
        })
        .filter(|path| {
            fs::read_to_string(path).is_ok_and(|content| content.starts_with("# Run memory:"))
        })
        .collect::<Vec<_>>();
    files.sort();
    if files.is_empty() {
        return Ok(());
    }
    let mut next_id = fs::read(run_dir.join("run.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .and_then(|record| {
            record
                .get("steps")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
        })
        .unwrap_or(1);
    for path in &files {
        let Ok(content) = fs::read_to_string(path) else {
            continue;
        };
        for line in content.lines() {
            if let Some(id) = line
                .strip_prefix("### ")
                .and_then(|line| line.split_once(". Verlauf:").map(|(id, _)| id))
                .and_then(|id| id.parse::<usize>().ok())
            {
                next_id = next_id.max(id + 1);
            }
        }
    }
    let (incoming_step, incoming_attempt) = incoming_step_attempt(run_dir, step_id, attempt);
    for path in files {
        let content = fs::read_to_string(&path)
            .map_err(|error| format!("could not read run memory for history: {error}"))?;
        let memory_agent = content
            .lines()
            .find_map(|line| line.strip_prefix("# Run memory: "))
            .unwrap_or_default();
        let mut references = Vec::new();
        let mut findings = Vec::new();
        let mut matching_handoff = false;
        for line in content.lines() {
            if line.starts_with("## ") && line.contains(". Handoff:") {
                matching_handoff = line
                    .contains(&format!("Step `{incoming_step}` → Agent `{memory_agent}`"))
                    && line.contains(&format!("/ Step `{step_id}`"))
                    && line.contains(&format!("(Versuch {incoming_attempt})"));
                if matching_handoff && let Some((id, _)) = line.split_once(". Handoff:") {
                    references.push(id.trim_start_matches("## ").to_owned());
                }
            } else if line.starts_with("## ") {
                matching_handoff = false;
            } else if matching_handoff {
                let finding_id = line
                    .strip_prefix("### ")
                    .and_then(|rest| rest.split_whitespace().next())
                    .or_else(|| {
                        line.strip_prefix("- Finding ")
                            .and_then(|rest| rest.split_once(':').map(|(id, _)| id))
                    });
                if let Some(id) = finding_id.filter(|id| id.contains('.')) {
                    findings.push(id.to_owned());
                }
            }
        }
        references.sort();
        references.dedup();
        findings.sort();
        findings.dedup();
        let reference_text = if references.is_empty() {
            String::new()
        } else {
            format!(
                "; Handoff {}{}",
                references.join(", "),
                if findings.is_empty() {
                    String::new()
                } else {
                    format!("; Findings {}", findings.join(", "))
                }
            )
        };
        let entry = format!(
            "\n\n## Verlauf – Fortsetzung {next_id}\n\n### {next_id}. Verlauf: Step `{step_id}` (Versuch {attempt}) — Outcome `{outcome}`{reference_text}\n"
        );
        use std::io::Write as _;
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .map_err(|error| format!("could not append pipeline history: {error}"))?;
        file.write_all(entry.as_bytes())
            .map_err(|error| format!("could not append pipeline history: {error}"))?;
    }
    Ok(())
}

fn incoming_step_attempt(run_dir: &std::path::Path, step_id: &str, attempt: u32) -> (String, u32) {
    let record = fs::read(run_dir.join("run.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok());
    let Some(steps) = record
        .as_ref()
        .and_then(|record| record.get("steps"))
        .and_then(serde_json::Value::as_array)
    else {
        return (step_id.to_owned(), attempt);
    };
    let current = steps.iter().rposition(|step| {
        step.get("step_id").and_then(serde_json::Value::as_str) == Some(step_id)
            && step.get("attempt").and_then(serde_json::Value::as_u64) == Some(attempt as u64)
    });
    current
        .and_then(|index| index.checked_sub(1).map(|previous| &steps[previous]))
        .and_then(|step| {
            Some((
                step.get("step_id")?.as_str()?.to_owned(),
                step.get("attempt")?.as_u64()? as u32,
            ))
        })
        .unwrap_or_else(|| (step_id.to_owned(), attempt))
}

fn prompt_value(prompt: &str) -> Result<String, String> {
    use std::io::{self, Write};

    print!("{prompt}");
    io::stdout()
        .flush()
        .map_err(|error| format!("could not display prompt: {error}"))?;
    let mut value = String::new();
    let bytes = io::stdin()
        .read_line(&mut value)
        .map_err(|error| format!("could not read input: {error}"))?;
    if bytes == 0 {
        return Err("no input received (stdin is closed)".into());
    }
    let value = value.trim_end_matches(['\r', '\n']).to_owned();
    if value.trim().is_empty() {
        return Err("input must not be empty".into());
    }
    Ok(value)
}

fn task_for_run(task: &str, inputs: &HashMap<String, String>) -> String {
    if !task.trim().is_empty() {
        return task.to_owned();
    }
    if let Some(task) = inputs.get("task") {
        return task.clone();
    }
    let mut entries: Vec<_> = inputs.iter().collect();
    entries.sort_by_key(|(key, _)| *key);
    entries
        .into_iter()
        .map(|(key, value)| format!("{key}: {value}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn validate_pipeline_inputs(
    pipeline: &Pipeline,
    values: &HashMap<String, String>,
) -> Result<(), String> {
    for input in &pipeline.inputs {
        if !values.contains_key(&input.key) {
            return Err(format!(
                "required input `{}` ({}) was not provided",
                input.key, input.description
            ));
        }
    }
    for name in values.keys() {
        if !pipeline.inputs.iter().any(|input| &input.key == name) {
            return Err(format!(
                "pipeline `{}` does not declare input `{name}`",
                pipeline.name
            ));
        }
    }
    Ok(())
}

fn shell_config(root: &std::path::Path, inputs: HashMap<String, String>) -> ShellRunnerConfig {
    let mut config = ShellRunnerConfig::new(root, SHELL_TIMEOUT);
    config.inputs = inputs;
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
    fn is_cancelled(&self) -> bool {
        self.store.is_cancelled()
    }

    fn step_started(&mut self, step_id: &str) -> Result<(), String> {
        self.store.step_started(step_id)?;
        let record = self.store.record();
        let step = record.steps.last().expect("step was just recorded");
        println!(
            "Starting step `{step_id}` (Step {}/{MAX_STEP_EXECUTIONS}, attempt {})",
            record.steps.len(),
            step.attempt
        );
        Ok(())
    }

    fn step_completed(&mut self, result: &StepResult) -> Result<(), String> {
        self.store.step_completed(result)?;
        let attempt = self
            .store
            .record()
            .steps
            .last()
            .filter(|attempt| attempt.step_id == result.step_id)
            .map_or(1, |attempt| attempt.attempt);
        append_pipeline_history(
            self.store.run_dir(),
            &result.step_id,
            attempt,
            &result.outcome,
        )?;
        println!("Completed step `{}` ({})", result.step_id, result.outcome);
        Ok(())
    }

    fn human_answer(
        &mut self,
        step_id: &str,
        prompt: &str,
        options: &[factory_core::HumanOption],
    ) -> Result<String, String> {
        self.store
            .wait_for_human(step_id, prompt, options)
            .map_err(|e| e.to_string())?;
        if !io::stdin().is_terminal() {
            return Err("Human step is waiting for input; resume this run to answer".into());
        }
        println!("{prompt}");
        for option in options {
            println!("  {}: {}", option.id, option.label);
        }
        loop {
            eprint!("Answer: ");
            io::stderr().flush().map_err(|error| error.to_string())?;
            let answer = read_interactive_line_or_cancel(self.store)?;
            if answer.is_empty() {
                return Err("stdin is closed; human question remains pending".into());
            }
            let answer = answer.trim().to_owned();
            if answer.is_empty() {
                eprintln!("Human answer must not be empty.");
                continue;
            }
            if !options.is_empty() && !options.iter().any(|option| option.id == answer) {
                eprintln!(
                    "Choose one of: {}",
                    options
                        .iter()
                        .map(|option| option.id.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                continue;
            }
            return Ok(answer);
        }
    }

    fn step_failed(&mut self, step_id: &str, error: &str) -> Result<(), String> {
        self.store.step_failed(step_id, error)?;
        let attempt = self
            .store
            .record()
            .steps
            .last()
            .filter(|attempt| attempt.step_id == step_id)
            .map_or(1, |attempt| attempt.attempt);
        append_pipeline_history(self.store.run_dir(), step_id, attempt, "failure")?;
        eprintln!("Step `{step_id}` execution error: {error}");
        Ok(())
    }

    fn confirm_repeated_review_feedback(&mut self, result: &StepResult) -> Result<bool, String> {
        eprintln!(
            "Warning: identical reviewer feedback has occurred again on `{}`.",
            result.step_id
        );
        if !io::stdin().is_terminal() {
            eprintln!(
                "Non-interactive input: stopping safely. After addressing the feedback, continue with `factory run resume {} --retry-current`.",
                self.store.record().run_id
            );
            return Ok(false);
        }
        eprint!("Continue review loop? [y/N] ");
        io::stderr().flush().map_err(|error| error.to_string())?;
        let mut answer = String::new();
        io::stdin()
            .read_line(&mut answer)
            .map_err(|error| error.to_string())?;
        Ok(matches!(
            answer.trim().to_ascii_lowercase().as_str(),
            "y" | "yes"
        ))
    }
}

fn print_report(report: &RunReport, pipeline: &Pipeline) -> u8 {
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
            terminal_failure_exit_code(report, pipeline)
        }
    }
}

fn terminal_failure_exit_code(report: &RunReport, pipeline: &Pipeline) -> u8 {
    let Some(terminal) = report.steps.last() else {
        return EXIT_EXECUTION_ERROR;
    };
    let Some(step) = pipeline
        .steps
        .iter()
        .find(|step| step.id == terminal.step_id)
    else {
        return EXIT_EXECUTION_ERROR;
    };
    match step.kind {
        StepKind::Shell { .. } | StepKind::Human { .. } => EXIT_EXECUTION_ERROR,
        StepKind::Agent { .. } => EXIT_REVIEW_FAILED,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use factory_core::{Step, features::RunError};

    #[test]
    fn pipeline_history_is_numbered_chronological_and_shared_by_agent_memories() {
        let root =
            std::env::temp_dir().join(format!("factory-pipeline-history-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("01-agent.md"),
            "# Run memory: agent\n\n## Verlauf\n",
        )
        .unwrap();
        fs::write(
            root.join("02-agent.md"),
            "# Run memory: coder\n\n## 1. Handoff: Step `review` → Agent `coder` / Step `build` (Versuch 1)\n\n### 1.1 Fix issue\n",
        )
        .unwrap();
        fs::write(
            root.join("handoff-review.md"),
            "artifact must remain untouched\n",
        )
        .unwrap();

        append_pipeline_history(&root, "review", 1, "changes_requested").unwrap();
        fs::write(
            root.join("run.json"),
            r#"{"steps":[{"step_id":"review","attempt":1},{"step_id":"build","attempt":2}]}"#,
        )
        .unwrap();
        append_pipeline_history(&root, "build", 2, "success").unwrap();
        let first = fs::read_to_string(root.join("01-agent.md")).unwrap();
        let second = fs::read_to_string(root.join("02-agent.md")).unwrap();
        for content in [&first, &second] {
            assert!(content.contains("## Verlauf – Fortsetzung 1\n\n### 1. Verlauf: Step `review` (Versuch 1) — Outcome `changes_requested`"));
            assert!(content.contains("## Verlauf – Fortsetzung 2\n\n### 2. Verlauf: Step `build` (Versuch 2) — Outcome `success`"));
            assert!(content.find("Step `review`").unwrap() < content.find("Step `build`").unwrap());
        }
        assert_eq!(first.matches("### 1. Verlauf:").count(), 1);
        assert!(!first.contains("Handoff 1"));
        assert!(second.contains("Handoff 1; Findings 1.1"));
        assert_eq!(
            fs::read_to_string(root.join("handoff-review.md")).unwrap(),
            "artifact must remain untouched\n"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn pipeline_history_includes_agent_memories_with_three_digit_indices() {
        let root =
            std::env::temp_dir().join(format!("factory-many-memories-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        for index in 0..=100 {
            fs::write(
                root.join(format!("{index:02}-agent-{index}.md")),
                format!("# Run memory: agent-{index}\n\n## Verlauf\n"),
            )
            .unwrap();
        }
        append_pipeline_history(&root, "build", 1, "success").unwrap();
        let hundredth = fs::read_to_string(root.join("100-agent-100.md")).unwrap();
        assert!(hundredth.contains("### 1. Verlauf: Step `build` (Versuch 1) — Outcome `success`"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn pipeline_history_references_only_the_incoming_cycle_and_local_findings() {
        let root =
            std::env::temp_dir().join(format!("factory-cycle-references-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("run.json"), r#"{"steps":[{"step_id":"review","attempt":1},{"step_id":"build","attempt":1},{"step_id":"review","attempt":2},{"step_id":"build","attempt":2}]}"#).unwrap();
        fs::write(root.join("01-reviewer.md"), "# Run memory: reviewer\n\n## 7. Handoff: Step `review` → Agent `coder` / Step `build` (Versuch 2)\n\nThe complete result is stored in an artifact.\n- Finding 7.1: Foreign local finding\n").unwrap();
        fs::write(root.join("02-coder.md"), "# Run memory: coder\n\n## 3. Handoff: Step `review` → Agent `coder` / Step `build` (Versuch 2)\n\nThe complete result is stored in an artifact.\n- Finding 3.4: Current external finding\n- Finding 3.5: Another current finding\n\n## 9. Handoff: Step `review` → Agent `other` / Step `build` (Versuch 2)\n\n- Finding 9.1: Wrong target finding\n").unwrap();
        append_pipeline_history(&root, "build", 2, "success").unwrap();
        let reviewer = fs::read_to_string(root.join("01-reviewer.md")).unwrap();
        let coder = fs::read_to_string(root.join("02-coder.md")).unwrap();
        let reviewer_history = reviewer
            .lines()
            .find(|line| line.contains("Verlauf: Step `build`"))
            .unwrap();
        let coder_history = coder
            .lines()
            .find(|line| line.contains("Verlauf: Step `build`"))
            .unwrap();
        assert!(!reviewer_history.contains("Handoff") && !reviewer_history.contains("Findings"));
        assert!(
            coder_history.contains("Handoff 3; Findings 3.4, 3.5"),
            "{coder_history}"
        );
        assert!(!coder_history.contains("Handoff 7") && !coder_history.contains("Findings 7."));
        assert!(!coder_history.contains("Handoff 9"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn validates_declared_pipeline_inputs_generically() {
        let pipeline = Pipeline {
            name: "feature".into(),
            inputs: vec![factory_core::PipelineInput {
                key: "target".into(),
                description: "Target input".into(),
            }],
            steps: vec![],
        };
        assert!(validate_pipeline_inputs(&pipeline, &HashMap::new()).is_err());
        assert!(
            validate_pipeline_inputs(
                &pipeline,
                &HashMap::from([("target".into(), "value".into())])
            )
            .is_ok()
        );
        assert!(
            validate_pipeline_inputs(
                &pipeline,
                &HashMap::from([("unknown".into(), "value".into())])
            )
            .is_err()
        );
    }

    #[test]
    fn distinguishes_terminal_failure_from_execution_errors() {
        let pipeline = Pipeline {
            name: "review".into(),
            inputs: vec![],
            steps: vec![agent_step("review", "reviewer")],
        };
        let report = RunReport {
            pipeline_name: "review".into(),
            status: RunStatus::Failed,
            steps: vec![StepResult {
                step_id: "review".into(),
                outcome: "failure".into(),
                output: String::new(),
                exit_code: None,
                runner_attempts: 1,
                handoff: None,
            }],
        };
        assert_eq!(print_report(&report, &pipeline), EXIT_REVIEW_FAILED);
        let shell_pipeline = Pipeline {
            name: "shell".into(),
            inputs: vec![],
            steps: vec![Step {
                id: "command".into(),
                kind: StepKind::Shell {
                    command: "false".into(),
                    args: vec![],
                },
                transitions: vec![],
            }],
        };
        let shell_report = RunReport {
            pipeline_name: "shell".into(),
            status: RunStatus::Failed,
            steps: vec![StepResult {
                step_id: "command".into(),
                outcome: "failure".into(),
                output: String::new(),
                exit_code: Some(1),
                runner_attempts: 1,
                handoff: None,
            }],
        };
        assert_eq!(
            print_report(&shell_report, &shell_pipeline),
            EXIT_EXECUTION_ERROR
        );
        assert_eq!(
            terminal_failure_exit_code(
                &report,
                &Pipeline {
                    steps: vec![],
                    ..pipeline.clone()
                }
            ),
            EXIT_EXECUTION_ERROR
        );
        let unknown_report = RunReport {
            steps: vec![StepResult {
                step_id: "unknown".into(),
                ..report.steps[0].clone()
            }],
            ..report.clone()
        };
        assert_eq!(
            terminal_failure_exit_code(&unknown_report, &pipeline),
            EXIT_EXECUTION_ERROR
        );
        let success = RunReport {
            status: RunStatus::Succeeded,
            ..report
        };
        assert_eq!(print_report(&success, &pipeline), EXIT_SUCCESS);
    }

    #[test]
    fn configuration_errors_return_exit_code_two_from_pipeline_entrypoint() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("factory-config-error-{unique}"));
        fs::create_dir_all(&root).unwrap();
        assert_eq!(
            run_new_pipeline_from_root(&root, "missing", "task", HashMap::new()),
            EXIT_CONFIGURATION_ERROR
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_human_pause_persistence_is_an_execution_error() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("factory-human-persist-{unique}"));
        fs::create_dir_all(&root).unwrap();
        let mut store = test_store(&root, "human");
        let run_dir = store.run_dir().to_path_buf();
        fs::remove_dir_all(&run_dir).unwrap();
        assert!(store.wait_for_human("ask", "Choose", &[]).is_err());
        assert_ne!(store.record().status, PersistedRunStatus::WaitingForInput);
        assert_eq!(
            finish_pipeline_result(
                &mut store,
                Err(RunError::RunnerFailed {
                    step_id: "ask".into(),
                    message: "could not persist pending question".into(),
                }),
                &Pipeline {
                    name: "human".into(),
                    inputs: vec![],
                    steps: vec![]
                },
            ),
            EXIT_EXECUTION_ERROR
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn productive_pipeline_execution_maps_shell_and_agent_failures_to_exit_codes() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("factory-exit-codes-{unique}"));
        fs::create_dir_all(&root).unwrap();
        let agent_pipeline = Pipeline {
            name: "review".into(),
            inputs: vec![],
            steps: vec![agent_step("review", "reviewer")],
        };
        let mut store = test_store(&root, "review");
        let mut agents = FailedAgent;
        let mut shell = FailedShell;
        assert_eq!(
            execute_pipeline_and_finish(
                &mut store,
                &agent_pipeline,
                &[agent("reviewer")],
                &mut agents,
                &mut shell,
                &[],
            ),
            EXIT_REVIEW_FAILED
        );

        let shell_pipeline = Pipeline {
            name: "shell".into(),
            inputs: vec![],
            steps: vec![Step {
                id: "shell".into(),
                kind: StepKind::Shell {
                    command: "false".into(),
                    args: vec![],
                },
                transitions: vec![],
            }],
        };
        let mut store = test_store(&root, "shell");
        let mut agents = FailedAgent;
        let mut shell = ShellRunnerAdapter::new(shell_config(&root, HashMap::new()));
        assert_eq!(
            execute_pipeline_and_finish(
                &mut store,
                &shell_pipeline,
                &[],
                &mut agents,
                &mut shell,
                &[],
            ),
            EXIT_EXECUTION_ERROR
        );

        let mut store = test_store(&root, "runner");
        assert_eq!(
            finish_pipeline_result(
                &mut store,
                Err(RunError::RunnerFailed {
                    step_id: "run".into(),
                    message: "failed".into()
                }),
                &shell_pipeline,
            ),
            EXIT_EXECUTION_ERROR
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn derives_run_task_from_pipeline_inputs_without_prompting_for_task() {
        let inputs =
            HashMap::from([("issue_url".into(), "https://github.com/o/r/issues/1".into())]);
        assert_eq!(
            task_for_run("", &inputs),
            "issue_url: https://github.com/o/r/issues/1"
        );
        assert_eq!(task_for_run("Explicit task", &inputs), "Explicit task");
        assert_eq!(
            task_for_run("", &HashMap::from([("task".into(), "Do it".into())])),
            "Do it"
        );
    }

    #[test]
    fn accepts_pipeline_name_without_task_and_rejects_missing_input_values() {
        assert_eq!(run_cli(&[]), EXIT_USAGE_ERROR);
        assert_eq!(
            run_cli(&["feature".into(), "--input".into()]),
            EXIT_USAGE_ERROR
        );
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
            inputs: vec![factory_core::PipelineInput {
                key: "task".into(),
                description: "Task to run".into(),
            }],
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
            HashMap::from([("task".into(), "test task".into())]),
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
        store.finish(PersistedRunStatus::Failed).unwrap();

        assert_eq!(
            resume_run(&root, &run_dir, &mut store, false),
            EXIT_USAGE_ERROR
        );
        assert_eq!(resume_run(&root, &run_dir, &mut store, true), EXIT_SUCCESS);
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
            inputs: vec![],
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
            let contents = fs::read_to_string(path).unwrap();
            assert!(contents.contains("Add the feature"));
            assert!(contents.contains("## Verlauf\n\n<!-- Chronologische Einträge append-only"));
        }
        let existing_path = memories.get("coder").unwrap();
        let existing = "# Existing run memory\n\n## Verlauf\n\n### 1. Verlauf: preserved\n";
        fs::write(existing_path, existing).unwrap();
        initialize_agent_memories(
            existing_path.parent().unwrap(),
            &pipeline,
            &agents,
            "replacement task",
        )
        .unwrap();
        assert_eq!(fs::read_to_string(existing_path).unwrap(), existing);
        let run_dir = memories.values().next().unwrap().parent().unwrap();
        assert_eq!(fs::read_dir(run_dir).unwrap().count(), 2);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_step_completion_does_not_append_successful_markdown_history() {
        let root =
            std::env::temp_dir().join(format!("factory-history-failed-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let mut store = test_store(&root, "run");
        let memory = root.join("run").join("agent.md");
        fs::write(&memory, "# Agent\n\n## Verlauf\n").unwrap();
        store.step_started("build").unwrap();
        let run_dir = root.join("run");
        fs::remove_file(run_dir.join("run.json")).unwrap();
        fs::create_dir(run_dir.join("run.json")).unwrap();
        let result = StepResult {
            step_id: "build".into(),
            outcome: "success".into(),
            output: String::new(),
            exit_code: None,
            runner_attempts: 1,
            handoff: None,
        };
        let mut journal = ConsoleRunJournal::new(&mut store);
        assert!(journal.step_completed(&result).is_err());
        let saved = fs::read_to_string(memory).unwrap();
        assert!(!saved.contains("Outcome `success`"));
        fs::remove_dir_all(root).unwrap();
    }

    fn test_store(root: &std::path::Path, id: &str) -> factory_adapters::run_store::RunStore {
        let run_dir = root.join(id);
        fs::create_dir_all(&run_dir).unwrap();
        RunStore::create(
            &run_dir,
            id.into(),
            id.into(),
            "test".into(),
            HashMap::new(),
        )
        .unwrap()
    }

    struct FailedAgent;

    impl factory_core::features::AgentRunner for FailedAgent {
        fn run(
            &mut self,
            _agent: &factory_core::AgentDefinition,
            _step: &Step,
            _previous_result: Option<&StepResult>,
        ) -> Result<factory_core::features::StepOutput, String> {
            Ok(factory_core::features::StepOutput {
                outcome: "failure".into(),
                output: "review rejected".into(),
                exit_code: None,
                attempts: 1,
                handoff: None,
            })
        }
    }

    struct FailedShell;

    impl factory_core::features::ShellRunner for FailedShell {
        fn run(
            &mut self,
            _step: &Step,
            _previous_result: Option<&StepResult>,
        ) -> Result<factory_core::features::StepOutput, String> {
            Err("command failed".into())
        }
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
                context: vec![],
            },
            transitions: vec![],
        }
    }
}
