//! The log file is rolled by size, with a set number of earlier files kept.
//! This runs the real binary, as the log is set up by `main`.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        let path =
            std::env::temp_dir().join(format!("rexyra-logfile-{name}-{}", std::process::id()));
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

/// Starts the server, waits until it answers, and shuts it down, so that it
/// writes a few lines to its log. Returns its process id.
fn run_once(dir: &TempDir, max_size: &str, max_files: &str) -> u32 {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_rexyra-server"))
        .args(["--port", &port.to_string(), "--dir"])
        .arg(&dir.0)
        .args(["--save", "", "--logfile-max-size", max_size])
        .args(["--logfile-max-files", max_files, "--logfile"])
        .arg(dir.0.join("rexyra.log"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let started = Instant::now();
    let mut stream = loop {
        if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
            break stream;
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "the server exited at start-up"
        );
        assert!(started.elapsed() < Duration::from_secs(10), "no server");
        std::thread::sleep(Duration::from_millis(20));
    };
    stream.write_all(b"SHUTDOWN NOSAVE\r\n").unwrap();
    let _ = BufReader::new(&mut stream).read_line(&mut String::new());
    assert!(child.wait().unwrap().success());
    child.id()
}

#[test]
fn the_log_is_rolled_and_the_oldest_files_removed() {
    let dir = TempDir::new("roll");
    // Each run writes more than this, so each one rolls the file at least once.
    let mut pids = Vec::new();
    for _ in 0..4 {
        pids.push(run_once(&dir, "100", "2"));
    }
    let read = |name: &str| std::fs::read_to_string(dir.0.join(name)).ok();
    let (current, first, second) = (
        read("rexyra.log").unwrap(),
        read("rexyra.1.log").unwrap(),
        read("rexyra.2.log").unwrap(),
    );
    assert_eq!(
        read("rexyra.3.log"),
        None,
        "only two earlier files are kept"
    );
    // The last run's start is in the newest files; the first run's is gone.
    let started = |pid: u32| format!("pid={pid}");
    let newest = format!("{second}{first}{current}");
    assert!(newest.contains(&started(pids[3])), "{newest}");
    assert!(!newest.contains(&started(pids[0])), "{newest}");
    // A file holds more than the limit only if a single line does.
    for text in [&current, &first, &second] {
        assert!(
            text.len() <= 100 || text.trim_end().lines().count() == 1,
            "{text}"
        );
    }
}

#[test]
fn bad_settings_are_refused() {
    let dir = TempDir::new("bad");
    let status = Command::new(env!("CARGO_BIN_EXE_rexyra-server"))
        .args(["--logfile-max-files", "many", "--logfile"])
        .arg(dir.0.join("rexyra.log"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(2));
}
