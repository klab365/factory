use std::{
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("factory-cli-cancel-{unique}"));
        fs::create_dir_all(root.join(".factory/pipelines")).unwrap();
        fs::create_dir_all(root.join(".factory/agents")).unwrap();
        Self(root)
    }

    fn start_long_running_run(&self, pid_file: &str) -> Child {
        let pipeline = format!(
            "name = 'cancel-test'\n[[inputs]]\nkey = 'task'\ndescription = 'Task'\n[[steps]]\nid = 'long'\ntype = 'shell'\ncommand = '/bin/sh'\nargs = ['-c', 'sleep 60 & echo $! > {pid_file}; exit 0']\n"
        );
        fs::write(self.0.join(".factory/pipelines/cancel-test.toml"), pipeline).unwrap();
        Command::new(env!("CARGO_BIN_EXE_factory"))
            .args(["cancel-test", "--input", "task=test"])
            .current_dir(&self.0)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    }

    fn run_dir(&self) -> PathBuf {
        fs::read_dir(self.0.join(".factory/runs"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn separate_cli_cancel_stops_descendant_after_shell_parent_exits_and_persists_status() {
    let fixture = Fixture::new();
    let pid_file = fixture.0.join("child.pid");
    let mut run = fixture.start_long_running_run(pid_file.to_str().unwrap());
    let deadline = Instant::now() + Duration::from_secs(10);
    while !pid_file.exists() && Instant::now() < deadline {
        assert!(
            run.try_wait().unwrap().is_none(),
            "run exited before inherited output child started"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert!(pid_file.exists(), "shell child did not start");
    let run_dir = fixture.run_dir();
    let run_id = run_dir.file_name().unwrap().to_string_lossy().into_owned();
    let child_pid: u32 = fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    let cancelled = Command::new(env!("CARGO_BIN_EXE_factory"))
        .args(["run", "cancel", &run_id])
        .current_dir(&fixture.0)
        .output()
        .unwrap();
    assert!(
        cancelled.status.success(),
        "{}",
        String::from_utf8_lossy(&cancelled.stderr)
    );
    let started = Instant::now();
    loop {
        if let Some(status) = run.try_wait().unwrap() {
            assert!(status.success(), "run process returned {status}");
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "run process did not exit promptly; record: {}",
            fs::read_to_string(run_dir.join("run.json")).unwrap_or_default()
        );
        thread::sleep(Duration::from_millis(20));
    }

    let record = fs::read_to_string(run_dir.join("run.json")).unwrap();
    assert!(record.contains("\"status\": \"cancelled\""), "{record}");
    assert!(record.contains("\"step_id\": \"long\""), "{record}");

    #[cfg(unix)]
    {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let alive = Command::new("kill")
                .args(["-0", &child_pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success());
            if !alive {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "shell descendant {child_pid} survived cancel"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }
}
