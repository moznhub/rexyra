//! The server refuses to start when its data folder (`dir`) is missing or is
//! a file, as Redis does, instead of starting and failing at every save.
//! This runs the real binary, as the check is made by `main`.

use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        let path =
            std::env::temp_dir().join(format!("rexyra-datafolder-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Runs the server with `args`, port 0, and expects it to exit by itself.
fn start(args: &[&std::ffi::OsStr]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rexyra-server"))
        .args(["--port", "0"])
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let started = Instant::now();
    while child.try_wait().unwrap().is_none() {
        if started.elapsed() > Duration::from_secs(10) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the server started instead of refusing");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    child.wait_with_output().unwrap()
}

#[test]
fn a_missing_data_folder_stops_the_server_at_start() {
    let temp = TempDir::new("missing");
    let missing = temp.0.join("not-there");
    let out = start(&["--dir".as_ref(), missing.as_os_str()]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(
        stderr.contains("does not exist") && stderr.contains("not-there"),
        "{stderr}"
    );
    assert!(!stderr.contains("ready to accept connections"), "{stderr}");
    assert!(!missing.exists(), "the folder must not be created");
}

#[test]
fn a_data_folder_that_is_a_file_stops_the_server_at_start() {
    let temp = TempDir::new("file");
    let file = temp.0.join("data");
    std::fs::write(&file, b"").unwrap();
    let out = start(&["--dir".as_ref(), file.as_os_str()]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(stderr.contains("is a file, not a folder"), "{stderr}");
}

#[test]
fn with_a_log_file_the_reason_goes_to_the_log_and_to_standard_error() {
    let temp = TempDir::new("logged");
    let missing = temp.0.join("not-there");
    let log = temp.0.join("rexyra.log");
    let out = start(&[
        "--dir".as_ref(),
        missing.as_os_str(),
        "--logfile".as_ref(),
        log.as_os_str(),
    ]);
    assert!(!out.status.success());
    let logged = std::fs::read_to_string(&log).unwrap();
    assert!(logged.contains("does not exist"), "{logged}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("does not exist"), "{stderr}");
}
