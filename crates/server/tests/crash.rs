//! What is on disk after the server process is killed in the middle of a
//! background save. This needs a process of its own to kill, so the tests
//! here run the real binary.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const KEYS_BEFORE: usize = 20_000;
const KEYS_AFTER: usize = 120_000;

/// A directory of its own for one test, removed when the test ends.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        let path = std::env::temp_dir().join(format!("rexyra-crash-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }

    fn files(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(&self.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A server process, killed when dropped.
struct Server {
    child: Child,
    port: u16,
}

impl Server {
    fn start(dir: &Path) -> Server {
        // A port that was free a moment ago.
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let child = Command::new(env!("CARGO_BIN_EXE_rexyra-server"))
            .args(["--port", &port.to_string(), "--dir"])
            .arg(dir)
            .args(["--save", ""])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Server { child, port }
    }

    fn connect(&mut self) -> Client {
        let started = Instant::now();
        loop {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", self.port)) {
                stream
                    .set_read_timeout(Some(Duration::from_secs(60)))
                    .unwrap();
                return Client {
                    reader: BufReader::new(stream.try_clone().unwrap()),
                    stream,
                };
            }
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!("the server exited at start-up: {status}");
            }
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "the server did not start"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn kill(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Client {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Client {
    /// Sends one command and returns the first line of its reply.
    fn ask(&mut self, command: &str) -> String {
        self.stream
            .write_all(format!("{command}\r\n").as_bytes())
            .unwrap();
        self.line()
    }

    fn line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        line.trim_end().to_owned()
    }

    fn bulk(&mut self, command: &str) -> String {
        let header = self.ask(command);
        let len: usize = header
            .strip_prefix('$')
            .unwrap_or_else(|| panic!("not a bulk string: {header}"))
            .parse()
            .unwrap();
        let mut body = vec![0u8; len + 2];
        self.reader.read_exact(&mut body).unwrap();
        body.truncate(len);
        String::from_utf8(body).unwrap()
    }

    /// Writes keys `from..to`: a hundred per command, ten commands per
    /// round trip.
    fn fill(&mut self, from: usize, to: usize) {
        let value = "v".repeat(200);
        for round in (from..to).step_by(1_000) {
            let mut request = String::new();
            let mut commands = 0;
            for start in (round..(round + 1_000).min(to)).step_by(100) {
                request.push_str("MSET");
                for n in start..(start + 100).min(to) {
                    request.push_str(&format!(" key:{n} {value}"));
                }
                request.push_str("\r\n");
                commands += 1;
            }
            self.stream.write_all(request.as_bytes()).unwrap();
            for _ in 0..commands {
                assert_eq!(self.line(), "+OK");
            }
        }
    }

    fn dbsize(&mut self) -> usize {
        self.ask("DBSIZE")[1..].parse().unwrap()
    }
}

#[test]
fn killing_the_server_during_a_background_save_keeps_the_previous_snapshot() {
    let dir = TempDir::new("bgsave");
    let mut server = Server::start(&dir.0);
    let mut c = server.connect();

    // A complete snapshot of the first keys ...
    c.fill(0, KEYS_BEFORE);
    assert_eq!(c.ask("SAVE"), "+OK");
    assert_eq!(dir.files(), ["dump.rdb"]);
    let good = std::fs::read(dir.0.join("dump.rdb")).unwrap();

    // ... then many more keys, a background save, and the end of the
    // process while that save is (in all likelihood) still writing.
    c.fill(KEYS_BEFORE, KEYS_AFTER);
    assert_eq!(c.ask("BGSAVE"), "+Background saving started");
    let in_progress = c.bulk("INFO persistence");
    server.kill();
    let killed_mid_save = in_progress.contains("rdb_bgsave_in_progress:1");

    // Whatever the timing, the snapshot is a whole one: the old one, or the
    // new one if the save got to the end first. Never something in between.
    let after = std::fs::read(dir.0.join("dump.rdb")).unwrap();
    if killed_mid_save {
        let leftovers: Vec<String> = dir
            .files()
            .into_iter()
            .filter(|name| name.starts_with("temp-"))
            .collect();
        if !leftovers.is_empty() {
            assert_eq!(
                after, good,
                "the snapshot changed although the save was not finished"
            );
        }
    }
    // A leftover that is certainly there, whenever the kill landed.
    std::fs::write(dir.0.join("temp-bg-4242.rdb.7"), b"half a snapshot").unwrap();

    // The next start loads that snapshot and clears the leftovers away.
    let mut server = Server::start(&dir.0);
    let mut c = server.connect();
    let keys = c.dbsize();
    if after == good {
        assert_eq!(keys, KEYS_BEFORE);
    } else {
        assert_eq!(keys, KEYS_AFTER);
    }
    assert_eq!(c.bulk("GET key:0").len(), 200);
    assert_eq!(dir.files(), ["dump.rdb"]);

    // And saving works again.
    c.fill(KEYS_AFTER, KEYS_AFTER + 1_000);
    assert_eq!(c.ask("BGSAVE"), "+Background saving started");
    let finished = Instant::now();
    while c
        .bulk("INFO persistence")
        .contains("rdb_bgsave_in_progress:1")
    {
        assert!(
            finished.elapsed() < Duration::from_secs(60),
            "the save never finished"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(c
        .bulk("INFO persistence")
        .contains("rdb_last_bgsave_status:ok"));
    assert_eq!(c.ask("SHUTDOWN NOSAVE"), "+OK");
    drop(server);
    let mut server = Server::start(&dir.0);
    assert_eq!(server.connect().dbsize(), keys + 1_000);
    println!(
        "killed mid-save: {killed_mid_save}; snapshot kept: {}",
        after == good
    );
}
