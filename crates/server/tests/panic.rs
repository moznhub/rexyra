//! An internal error (a panic) ends the server process with a distinctive
//! exit code, so that a service manager restarts it. This needs a process of
//! its own, so the tests here run the real binary; `DEBUG PANIC` stands in
//! for a bug.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// The exit code `main` uses after a panic.
const EXIT_INTERNAL_ERROR: i32 = 70;

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        let path = std::env::temp_dir().join(format!("rexyra-panic-{name}-{}", std::process::id()));
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

/// A server process, killed when dropped if still running.
struct Server {
    child: Child,
    port: u16,
}

impl Server {
    fn start(dir: &TempDir, debug: &str) -> Server {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let child = Command::new(env!("CARGO_BIN_EXE_rexyra-server"))
            .args(["--port", &port.to_string(), "--dir"])
            .arg(&dir.0)
            .args(["--save", "", "--enable-debug-command", debug, "--logfile"])
            .arg(dir.0.join("rexyra.log"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Server { child, port }
    }

    fn connect(&mut self) -> TcpStream {
        let started = Instant::now();
        loop {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", self.port)) {
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                return stream;
            }
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!("the server exited at start-up: {status}");
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "the server did not start"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// The exit code, waiting up to 10 seconds for the process to end.
    fn exit_code(&mut self) -> Option<i32> {
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(10) {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status.code();
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        None
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Sends one command and returns the first line of the reply ("" if the
/// connection was closed instead).
fn send(stream: &mut TcpStream, command: &str) -> String {
    stream
        .write_all(format!("{command}\r\n").as_bytes())
        .unwrap();
    let mut line = String::new();
    let _ = BufReader::new(stream).read_line(&mut line);
    line
}

#[test]
fn an_internal_error_ends_the_process_with_its_exit_code() {
    let dir = TempDir::new("exit");
    let mut server = Server::start(&dir, "local");
    let mut watcher = server.connect();
    assert_eq!(send(&mut watcher, "PING"), "+PONG\r\n");

    let mut client = server.connect();
    // The connection closes without a reply.
    assert_eq!(send(&mut client, "DEBUG PANIC"), "");
    assert_eq!(server.exit_code(), Some(EXIT_INTERNAL_ERROR));

    let log = std::fs::read_to_string(dir.0.join("rexyra.log")).unwrap();
    assert!(
        log.contains("internal error, stopping so that the server can be restarted")
            && log.contains("DEBUG PANIC called by client"),
        "{log}"
    );
}

#[test]
fn debug_is_refused_unless_allowed() {
    let dir = TempDir::new("refused");
    let mut server = Server::start(&dir, "no");
    let mut client = server.connect();
    assert!(send(&mut client, "DEBUG PANIC").starts_with("-ERR DEBUG command not allowed."));
    assert_eq!(send(&mut client, "PING"), "+PONG\r\n");
    assert!(server.child.try_wait().unwrap().is_none());
}
