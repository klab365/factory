use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("factory-human-resume-{unique}"));
        fs::create_dir_all(root.join(".factory/pipelines")).unwrap();
        fs::create_dir_all(root.join(".factory/agents")).unwrap();
        Self { root }
    }

    fn setup_pipeline(&self, mode: &str) -> PathBuf {
        let result_path = self.root.join("shell-result.json");
        let completed_marker = self.root.join("completed-step-ran");
        let options = if mode == "choice" {
            "options = [{ id = 'yes', label = 'Yes' }, { id = 'no', label = 'No' }]\n"
        } else {
            ""
        };
        let pipeline = format!(
            "name = 'human-test'\n[[inputs]]\nkey = 'task'\ndescription = 'Task'\n[[steps]]\nid = 'before'\ntype = 'shell'\ncommand = '/bin/mkdir'\nargs = ['{}']\n[[steps.transitions]]\noutcome = 'success'\ntarget_step = 'ask'\n[[steps]]\nid = 'ask'\ntype = 'human'\nprompt = 'Stored question?'\n{options}[[steps.transitions]]\noutcome = 'success'\ntarget_step = 'after'\n[[steps]]\nid = 'after'\ntype = 'shell'\ncommand = '/bin/sh'\nargs = ['-c', 'printf \"%s\" \"$FACTORY_PREVIOUS_STEP_RESULT\" > {}']\n",
            completed_marker.display(),
            result_path.display()
        );
        fs::write(
            self.root.join(".factory/pipelines/human-test.toml"),
            pipeline,
        )
        .unwrap();
        result_path
    }

    fn factory(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_factory"))
            .args(args)
            .current_dir(&self.root)
            .output()
            .unwrap()
    }

    fn run_new(&self) -> Output {
        self.factory(&["human-test", "--input", "task=test"])
    }

    fn resume_pty(&self, run_id: &str, retry: bool, input: Option<&str>) -> Output {
        let script = r#"
import os, pty, select, subprocess, sys, time, errno
command = sys.argv[1:]
master, slave = pty.openpty()
child = subprocess.Popen(command, stdin=slave, stdout=slave, stderr=slave, close_fds=True)
os.close(slave)
data = bytearray()
sent = False
start = time.monotonic()
while child.poll() is None:
    ready, _, _ = select.select([master], [], [], 0.1)
    if ready:
        try:
            chunk = os.read(master, 4096)
        except OSError as error:
            if error.errno == errno.EIO: break
            raise
        if not chunk: break
        data.extend(chunk)
        if not sent and b'Answer: ' in data:
            payload = os.environ.get('FACTORY_TEST_INPUT', '')
            os.write(master, payload.encode())
            sent = True
    if time.monotonic() - start > 15:
        child.kill()
        raise RuntimeError('timed out waiting for CLI')
try:
    child.wait(timeout=3)
except subprocess.TimeoutExpired:
    child.kill(); child.wait()
    raise RuntimeError('CLI did not exit')
sys.stdout.buffer.write(data)
sys.exit(child.returncode)
"#;
        let mut args = vec!["run", "resume", run_id];
        if retry {
            args.push("--retry-current");
        }
        let mut command = Command::new("python3");
        command
            .args(["-c", script, env!("CARGO_BIN_EXE_factory")])
            .args(args)
            .current_dir(&self.root);
        if let Some(input) = input {
            command.env("FACTORY_TEST_INPUT", input);
        }
        command.output().expect("python3 PTY driver")
    }

    fn cancel_pty(&self, args: &[&str], run_id: Option<&str>) -> Output {
        let script = r#"
import os, pty, select, subprocess, sys, time, errno
factory = sys.argv[1]
args = sys.argv[2:-1]
master, slave = pty.openpty()
child = subprocess.Popen([factory, *args], stdin=slave, stdout=slave, stderr=slave, close_fds=True)
os.close(slave)
data = bytearray()
start = time.monotonic()
while b'Answer: ' not in data:
    if child.poll() is not None: raise RuntimeError('CLI exited before prompting: ' + repr(bytes(data)))
    ready, _, _ = select.select([master], [], [], 0.1)
    if ready:
        try: data.extend(os.read(master, 4096))
        except OSError as error:
            if error.errno == errno.EIO: break
            raise
    if time.monotonic() - start > 10: raise RuntimeError('timed out waiting for prompt')
runs = os.path.join(os.getcwd(), '.factory', 'runs')
run_id = sys.argv[-1] if sys.argv[-1] != '-' else None
if run_id is None:
    run_id = os.listdir(runs)[0]
cancel = subprocess.run([factory, 'run', 'cancel', run_id], cwd=os.getcwd(), capture_output=True, text=True)
if cancel.returncode != 0: raise RuntimeError('cancel failed: ' + cancel.stderr)
try: code = child.wait(timeout=3)
except subprocess.TimeoutExpired:
    child.kill(); child.wait()
    raise RuntimeError('interactive run did not exit after cancel')
print('child_exit=' + str(code))
"#;
        let mut command = Command::new("python3");
        command
            .args(["-c", script, env!("CARGO_BIN_EXE_factory")])
            .args(args)
            .arg(run_id.unwrap_or("-"))
            .current_dir(&self.root);
        command.output().expect("python3 cancellation PTY driver")
    }

    fn run_id(&self) -> String {
        fs::read_dir(self.root.join(".factory/runs"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .file_name()
            .to_string_lossy()
            .into_owned()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn verify_resume(mode: &str, answer: &str, expected_option: &str, use_retry: bool) {
    let fixture = Fixture::new();
    let result_path = fixture.setup_pipeline(mode);
    let initial = fixture.run_new();
    assert_eq!(initial.status.code(), Some(0), "{}", text(&initial));
    let run_id = fixture.run_id();
    let record_path = fixture
        .root
        .join(".factory/runs")
        .join(&run_id)
        .join("run.json");
    assert!(fixture.root.join("completed-step-ran").is_dir());
    let record = fs::read_to_string(&record_path).unwrap();
    assert!(record.contains("waiting_for_input"), "{record}");
    assert!(record.contains("Stored question?"), "{record}");

    let shown = fixture.factory(&["run", "show", &run_id, "--text"]);
    assert_eq!(shown.status.code(), Some(0), "{}", text(&shown));
    assert!(text(&shown).contains("Stored question?"));

    if mode == "choice" {
        let invalid = fixture.resume_pty(&run_id, use_retry, Some("invalid\n"));
        assert_eq!(invalid.status.code(), Some(64), "{}", text(&invalid));
        let waiting = fs::read_to_string(&record_path).unwrap();
        assert!(waiting.contains("waiting_for_input"), "{waiting}");
        assert!(waiting.contains("Stored question?"), "{waiting}");
    }
    let resumed = fixture.resume_pty(&run_id, use_retry, Some(&format!("{answer}\n")));
    assert_eq!(resumed.status.code(), Some(0), "{}", text(&resumed));
    assert!(text(&resumed).contains("Stored question?"));
    assert!(
        result_path.exists(),
        "following shell step did not run: {}",
        text(&resumed)
    );

    let handoff = fs::read_to_string(&result_path).unwrap();
    assert!(handoff.contains("\"kind\":\"human\""), "{handoff}");
    assert!(
        handoff.contains("\"prompt\":\"Stored question?\""),
        "{handoff}"
    );
    assert!(
        handoff.contains(&format!("\"answer\":\"{}\"", answer.trim())),
        "{handoff}"
    );
    if expected_option.is_empty() {
        assert!(handoff.contains("\"option_id\":null"), "{handoff}");
    } else {
        assert!(
            handoff.contains(&format!("\"option_id\":\"{expected_option}\"")),
            "{handoff}"
        );
    }

    let record = fs::read_to_string(record_path).unwrap();
    assert!(record.contains("\"status\": \"succeeded\""), "{record}");
    assert!(record.contains("\"answer\": \""), "{record}");
    let show = fixture.factory(&["run", "show", &run_id, "--text"]);
    assert!(text(&show).contains("succeeded"));
}

#[test]
fn freetext_pause_survives_restart_and_resumes_to_structured_shell_handoff() {
    verify_resume("text", "approved", "", false);
}

#[test]
fn choice_rejects_invalid_input_then_retry_resume_persists_structured_answer() {
    verify_resume("choice", "yes", "yes", true);
}

#[test]
fn cancel_exits_running_human_prompt_without_input() {
    let fixture = Fixture::new();
    fixture.setup_pipeline("text");
    let cancelled = fixture.cancel_pty(&["human-test", "--input", "task=test"], None);
    assert_eq!(cancelled.status.code(), Some(0), "{}", text(&cancelled));
    let run_id = fixture.run_id();
    let record_path = fixture
        .root
        .join(".factory/runs")
        .join(&run_id)
        .join("run.json");
    let record = fs::read_to_string(record_path).unwrap();
    assert_eq!(
        record.matches("\"status\": \"cancelled\"").count(),
        2,
        "{record}"
    );
    assert!(record.contains("\"pending_human\": null"), "{record}");
    let show = fixture.factory(&["run", "show", &run_id, "--text"]);
    assert!(
        !text(&show).contains("Waiting for input at"),
        "{}",
        text(&show)
    );
}

#[test]
fn cancel_exits_waiting_resume_prompt_without_input() {
    let fixture = Fixture::new();
    fixture.setup_pipeline("text");
    let initial = fixture.run_new();
    assert_eq!(initial.status.code(), Some(0), "{}", text(&initial));
    let run_id = fixture.run_id();
    let cancelled = fixture.cancel_pty(&["run", "resume", &run_id], Some(&run_id));
    assert_eq!(cancelled.status.code(), Some(0), "{}", text(&cancelled));
    let record = fs::read_to_string(
        fixture
            .root
            .join(".factory/runs")
            .join(&run_id)
            .join("run.json"),
    )
    .unwrap();
    assert!(record.contains("\"status\": \"cancelled\""), "{record}");
    assert!(record.contains("\"pending_human\": null"), "{record}");
    let show = fixture.factory(&["run", "show", &run_id, "--text"]);
    assert!(
        !text(&show).contains("Waiting for input at"),
        "{}",
        text(&show)
    );
}

#[test]
fn eof_during_resume_keeps_question_waiting() {
    let fixture = Fixture::new();
    fixture.setup_pipeline("text");
    let initial = fixture.run_new();
    assert_eq!(initial.status.code(), Some(0), "{}", text(&initial));
    let run_id = fixture.run_id();
    let resumed = fixture.resume_pty(&run_id, false, Some("\u{4}\u{4}"));
    assert_eq!(resumed.status.code(), Some(64), "{}", text(&resumed));
    let record = fs::read_to_string(
        fixture
            .root
            .join(".factory/runs")
            .join(run_id)
            .join("run.json"),
    )
    .unwrap();
    assert!(record.contains("waiting_for_input"), "{record}");
    assert!(record.contains("Stored question?"), "{record}");
}
