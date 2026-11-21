//! End-to-end tests for snapshots: a server is stopped and another started
//! on the same directory.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use rexyra_server::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);

/// A directory of its own for one test, removed when the test ends.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        let path =
            std::env::temp_dir().join(format!("rexyra-server-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }

    fn config(&self) -> Config {
        Config {
            dir: self.0.to_str().unwrap().to_owned(),
            ..Config::default()
        }
    }

    fn snapshot(&self) -> PathBuf {
        self.0.join("dump.rdb")
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A running server and the task it runs in.
struct Running {
    addr: SocketAddr,
    task: JoinHandle<std::io::Result<()>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

async fn start(config: Config) -> Running {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(rexyra_server::serve(listener, config, async {
        // Completes when the test asks for it, like Ctrl+C would.
        let _ = stopped.await;
    }));
    Running {
        addr,
        task,
        stop: Some(stop),
    }
}

impl Running {
    /// Waits for the server to stop on its own (after `SHUTDOWN`).
    async fn stopped(self) -> std::io::Result<()> {
        timeout(TIMEOUT, self.task)
            .await
            .expect("server did not stop")
            .unwrap()
    }

    /// Stops the server from outside, as Ctrl+C does.
    async fn interrupt(mut self) -> std::io::Result<()> {
        let _ = self.stop.take().unwrap().send(());
        self.stopped().await
    }
}

struct Client(TcpStream);

impl Client {
    async fn connect(addr: SocketAddr) -> Client {
        Client(TcpStream::connect(addr).await.unwrap())
    }

    async fn check(&mut self, command: &str, expected: &str) {
        self.0
            .write_all(format!("{command}\r\n").as_bytes())
            .await
            .unwrap();
        let mut got = vec![0u8; expected.len()];
        timeout(TIMEOUT, self.0.read_exact(&mut got))
            .await
            .expect("timed out waiting for reply")
            .expect("connection closed early");
        assert_eq!(String::from_utf8_lossy(&got), expected, "{command}");
    }
}

#[tokio::test]
async fn data_survives_save_and_restart() {
    let dir = TempDir::new("restart");
    let first = start(dir.config()).await;
    let mut c = Client::connect(first.addr).await;
    c.check("SET greeting hello", "+OK\r\n").await;
    c.check("RPUSH queue a b c", ":3\r\n").await;
    c.check("ZADD board 10 alice 20 bob", ":2\r\n").await;
    c.check("SET session token EX 1000", "+OK\r\n").await;
    c.check("HSET user name ann token t", ":2\r\n").await;
    c.check("HEXPIRE user 1000 FIELDS 1 token", "*1\r\n:1\r\n")
        .await;
    c.check("SAVE", "+OK\r\n").await;
    // Written after the save: not in the snapshot.
    c.check("SET unsaved lost", "+OK\r\n").await;
    c.check("SHUTDOWN NOSAVE", "+OK\r\n").await;
    first.stopped().await.unwrap();

    let second = start(dir.config()).await;
    let mut c = Client::connect(second.addr).await;
    c.check("GET greeting", "$5\r\nhello\r\n").await;
    c.check(
        "LRANGE queue 0 -1",
        "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
    )
    .await;
    c.check("ZSCORE board bob", "$2\r\n20\r\n").await;
    c.check("TTL session", ":1000\r\n").await;
    c.check("HTTL user FIELDS 2 token name", "*2\r\n:1000\r\n:-1\r\n")
        .await;
    c.check("EXISTS unsaved", ":0\r\n").await;
    c.check("DBSIZE", ":5\r\n").await;
}

#[tokio::test]
async fn shutdown_saves_and_stops_the_server() {
    let dir = TempDir::new("shutdown");
    let first = start(dir.config()).await;
    let mut c = Client::connect(first.addr).await;
    c.check("SET k v", "+OK\r\n").await;
    assert!(!dir.snapshot().exists());
    c.check("SHUTDOWN", "+OK\r\n").await;
    first.stopped().await.unwrap();
    assert!(dir.snapshot().exists());

    let second = start(dir.config()).await;
    let mut c = Client::connect(second.addr).await;
    c.check("GET k", "$1\r\nv\r\n").await;
}

#[tokio::test]
async fn an_interrupted_server_saves_what_changed() {
    let dir = TempDir::new("interrupt");
    let first = start(dir.config()).await;
    let mut c = Client::connect(first.addr).await;
    c.check("INCRBY visits 41", ":41\r\n").await;
    first.interrupt().await.unwrap();

    let second = start(dir.config()).await;
    let mut c = Client::connect(second.addr).await;
    c.check("INCR visits", ":42\r\n").await;
    // Nothing has been saved since; an interrupt with no changes pending
    // (after this save) leaves the file alone.
    c.check("SAVE", "+OK\r\n").await;
    let saved = std::fs::metadata(dir.snapshot())
        .unwrap()
        .modified()
        .unwrap();
    second.interrupt().await.unwrap();
    assert_eq!(
        std::fs::metadata(dir.snapshot())
            .unwrap()
            .modified()
            .unwrap(),
        saved
    );
}

#[tokio::test]
async fn functions_survive_a_restart() {
    let dir = TempDir::new("functions");
    let first = start(dir.config()).await;
    let mut c = Client::connect(first.addr).await;
    let library = "#!lua name=lib\nredis.register_function('answer', function() return 42 end)";
    let request = format!(
        "*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{library}\r\n",
        library.len()
    );
    c.0.write_all(request.as_bytes()).await.unwrap();
    let mut reply = [0u8; 9];
    timeout(TIMEOUT, c.0.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&reply, b"$3\r\nlib\r\n");
    c.check("SHUTDOWN SAVE", "+OK\r\n").await;
    first.stopped().await.unwrap();

    let second = start(dir.config()).await;
    let mut c = Client::connect(second.addr).await;
    c.check("FCALL answer 0", ":42\r\n").await;
}

#[tokio::test]
async fn a_damaged_snapshot_stops_the_server_from_starting() {
    let dir = TempDir::new("damaged");
    let first = start(dir.config()).await;
    let mut c = Client::connect(first.addr).await;
    c.check("SET k v", "+OK\r\n").await;
    c.check("SHUTDOWN", "+OK\r\n").await;
    first.stopped().await.unwrap();

    // Flip one byte in the middle of the file.
    let mut data = std::fs::read(dir.snapshot()).unwrap();
    let middle = data.len() / 2;
    data[middle] ^= 0xFF;
    std::fs::write(dir.snapshot(), &data).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let error = rexyra_server::serve(listener, dir.config(), std::future::pending())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cannot load"), "{error}");
    // The damaged file is left as it was, for the operator to look at.
    assert_eq!(std::fs::read(dir.snapshot()).unwrap(), data);
}

/// The `rdb_changes_since_last_save` field of `INFO persistence`.
async fn unsaved_changes(c: &mut Client) -> String {
    c.0.write_all(
        b"INFO persistence
",
    )
    .await
    .unwrap();
    let mut buffer = vec![0u8; 4096];
    let read = timeout(TIMEOUT, c.0.read(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    let text = String::from_utf8_lossy(&buffer[..read]).into_owned();
    text.lines()
        .find_map(|line| line.strip_prefix("rdb_changes_since_last_save:"))
        .unwrap_or_else(|| panic!("no field in {text}"))
        .trim()
        .to_owned()
}

#[tokio::test]
async fn info_reports_unsaved_changes() {
    let dir = TempDir::new("info");
    let server = start(dir.config()).await;
    let mut c = Client::connect(server.addr).await;
    c.check("SET a 1", "+OK\r\n").await;
    c.check("SET b 2", "+OK\r\n").await;
    assert_eq!(unsaved_changes(&mut c).await, "2");
    c.check("SAVE", "+OK\r\n").await;
    assert_eq!(unsaved_changes(&mut c).await, "0");
    // Not allowed inside a transaction, which is discarded by the attempt.
    c.check("MULTI", "+OK\r\n").await;
    c.check("SAVE", "-ERR Command not allowed inside a transaction\r\n")
        .await;
    c.check(
        "EXEC",
        "-EXECABORT Transaction discarded because of previous errors.\r\n",
    )
    .await;
}

/// Polls `LASTSAVE`-style state until no background save is running.
async fn wait_for_bgsave(c: &mut Client) {
    for _ in 0..500 {
        c.0.write_all(b"INFO persistence\r\n").await.unwrap();
        let mut buffer = vec![0u8; 4096];
        let read = timeout(TIMEOUT, c.0.read(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        if String::from_utf8_lossy(&buffer[..read]).contains("rdb_bgsave_in_progress:0") {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the background save never finished");
}

#[tokio::test]
async fn bgsave_under_load_is_a_consistent_point_in_time() {
    const PAIRS: usize = 200;
    let dir = TempDir::new("bgsave-load");
    let first = start(dir.config()).await;
    let addr = first.addr;
    let mut c = Client::connect(addr).await;
    // Enough keys that the save takes many slices.
    for chunk in 0..20 {
        let mut pipeline = String::new();
        let mut expected = String::new();
        for n in 0..1_000 {
            pipeline.push_str(&format!("SET filler:{chunk}:{n} {}\r\n", "x".repeat(100)));
            expected.push_str("+OK\r\n");
        }
        c.0.write_all(pipeline.as_bytes()).await.unwrap();
        let mut got = vec![0u8; expected.len()];
        timeout(TIMEOUT, c.0.read_exact(&mut got))
            .await
            .unwrap()
            .unwrap();
    }
    for pair in 0..PAIRS {
        c.check(&format!("MSET left:{pair} 0 right:{pair} 0"), "+OK\r\n")
            .await;
    }

    // Writers keep each pair equal with one atomic command per update, so
    // any point-in-time image has left == right for every pair.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writers: Vec<_> = (0..4)
        .map(|writer| {
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                let mut round = 1u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let pair = (round as usize * 7 + writer * 53) % PAIRS;
                    let value = round * 10 + writer as u64;
                    c.check(
                        &format!("MSET left:{pair} {value} right:{pair} {value}"),
                        "+OK\r\n",
                    )
                    .await;
                    round += 1;
                }
                round
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(30)).await;
    c.check("BGSAVE", "+Background saving started\r\n").await;
    wait_for_bgsave(&mut c).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut rounds = 0;
    for writer in writers {
        rounds += writer.await.unwrap();
    }
    assert!(rounds > 40, "writers barely ran: {rounds} rounds");
    c.check("SHUTDOWN NOSAVE", "+OK\r\n").await;
    first.stopped().await.unwrap();

    let second = start(dir.config()).await;
    let mut c = Client::connect(second.addr).await;
    c.check("DBSIZE", &format!(":{}\r\n", 20_000 + PAIRS * 2))
        .await;
    let mut changed = 0;
    for pair in 0..PAIRS {
        c.0.write_all(format!("MGET left:{pair} right:{pair}\r\n").as_bytes())
            .await
            .unwrap();
        // "*2\r\n$<n>\r\n<left>\r\n$<n>\r\n<right>\r\n"
        let mut reply = Vec::new();
        let mut byte = [0u8; 1];
        while reply.iter().filter(|&&b| b == b'\n').count() < 5 {
            timeout(TIMEOUT, c.0.read_exact(&mut byte))
                .await
                .unwrap()
                .unwrap();
            reply.push(byte[0]);
        }
        let text = String::from_utf8(reply).unwrap();
        let lines: Vec<&str> = text.split("\r\n").collect();
        assert_eq!(lines[2], lines[4], "pair {pair} was saved half-updated");
        if lines[2] != "0" {
            changed += 1;
        }
    }
    // The writers were running before the save began, so it saw their work.
    assert!(changed > 0);
}

#[tokio::test]
async fn automatic_saves_follow_the_save_points() {
    let dir = TempDir::new("autosave");
    let first = start(Config {
        // Save once at least one key changed and zero seconds have passed.
        save_points: vec![(0, 1)],
        ..dir.config()
    })
    .await;
    let mut c = Client::connect(first.addr).await;
    assert!(!dir.snapshot().exists());
    c.check("SET k v", "+OK\r\n").await;
    for _ in 0..300 {
        if dir.snapshot().exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(dir.snapshot().exists(), "no automatic save happened");
    wait_for_bgsave(&mut c).await;
    c.check("SHUTDOWN NOSAVE", "+OK\r\n").await;
    first.stopped().await.unwrap();

    let second = start(dir.config()).await;
    let mut c = Client::connect(second.addr).await;
    c.check("GET k", "$1\r\nv\r\n").await;
}

fn aof_config(dir: &TempDir) -> Config {
    let mut config = dir.config();
    config.aof.enabled = true;
    config.aof.fsync = rexyra_commands::aof::Fsync::Always;
    config
}

#[tokio::test]
async fn acknowledged_writes_survive_without_a_shutdown() {
    let dir = TempDir::new("aof-crash");
    let first = start(aof_config(&dir)).await;
    let mut c = Client::connect(first.addr).await;
    c.check("SET k v", "+OK\r\n").await;
    c.check("RPUSH l a b", ":2\r\n").await;
    c.check("SET temp v EX 1000", "+OK\r\n").await;
    c.check("HSET h a 1 b 2", ":2\r\n").await;
    c.check("HEXPIRE h 1000 FIELDS 1 a", "*1\r\n:1\r\n").await;
    c.check("MULTI", "+OK\r\n").await;
    c.check("INCR n", "+QUEUED\r\n").await;
    c.check("INCR n", "+QUEUED\r\n").await;
    c.check("EXEC", "*2\r\n:1\r\n:2\r\n").await;
    // No SAVE, no SHUTDOWN: the first server is simply left behind, as if
    // the process had died after replying.
    assert!(!dir.snapshot().exists());

    let second = start(aof_config(&dir)).await;
    let mut c = Client::connect(second.addr).await;
    c.check("GET k", "$1\r\nv\r\n").await;
    c.check("LRANGE l 0 -1", "*2\r\n$1\r\na\r\n$1\r\nb\r\n")
        .await;
    c.check("TTL temp", ":1000\r\n").await;
    c.check("HTTL h FIELDS 2 a b", "*2\r\n:1000\r\n:-1\r\n")
        .await;
    c.check("GET n", "$1\r\n2\r\n").await;
    drop(first);
}

#[tokio::test]
async fn the_log_can_be_rewritten_under_load() {
    const PAIRS: usize = 100;
    let dir = TempDir::new("aof-rewrite");
    let first = start(aof_config(&dir)).await;
    let addr = first.addr;
    let mut c = Client::connect(addr).await;
    for pair in 0..PAIRS {
        c.check(&format!("MSET left:{pair} 0 right:{pair} 0"), "+OK\r\n")
            .await;
    }
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writers: Vec<_> = (0..4)
        .map(|writer| {
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                let mut round = 1u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let pair = (round as usize * 7 + writer * 53) % PAIRS;
                    let value = round * 10 + writer as u64;
                    c.check(
                        &format!("MSET left:{pair} {value} right:{pair} {value}"),
                        "+OK\r\n",
                    )
                    .await;
                    round += 1;
                }
            })
        })
        .collect();

    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        c.check(
            "BGREWRITEAOF",
            "+Background append only file rewriting started\r\n",
        )
        .await;
        for _ in 0..500 {
            c.0.write_all(b"INFO persistence\r\n").await.unwrap();
            let mut buffer = vec![0u8; 4096];
            let read = timeout(TIMEOUT, c.0.read(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            if String::from_utf8_lossy(&buffer[..read]).contains("aof_rewrite_in_progress:0") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for writer in writers {
        writer.await.unwrap();
    }
    // What the live server holds now is what a restart must bring back.
    let mut expected = Vec::new();
    for pair in 0..PAIRS {
        c.0.write_all(format!("GET left:{pair}\r\n").as_bytes())
            .await
            .unwrap();
        let mut reply = Vec::new();
        let mut byte = [0u8; 1];
        while reply.iter().filter(|&&b| b == b'\n').count() < 2 {
            timeout(TIMEOUT, c.0.read_exact(&mut byte))
                .await
                .unwrap()
                .unwrap();
            reply.push(byte[0]);
        }
        expected.push(String::from_utf8(reply).unwrap());
    }

    let second = start(aof_config(&dir)).await;
    let mut c = Client::connect(second.addr).await;
    c.check("DBSIZE", &format!(":{}\r\n", PAIRS * 2)).await;
    for (pair, value) in expected.iter().enumerate() {
        c.check(&format!("GET left:{pair}"), value).await;
        c.check(&format!("GET right:{pair}"), value).await;
    }
    drop(first);
}

/// How a background save treats large collections: captured a piece at a
/// time, with a client that writes to one held back on its own while
/// everyone else carries on. Self-contained, with its own helpers.
mod large_collections {

    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    use rexyra_server::Config;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;

    const TIMEOUT: Duration = Duration::from_secs(30);
    /// Enough fields that capturing the hash takes a good many slices.
    const FIELDS: usize = 300_000;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> TempDir {
            let path = std::env::temp_dir()
                .join(format!("rexyra-server-large-{name}-{}", std::process::id()));
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

    async fn start(dir: &TempDir) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let config = Config {
            dir: dir.0.to_str().unwrap().to_owned(),
            save_points: Vec::new(),
            ..Config::default()
        };
        tokio::spawn(rexyra_server::serve(
            listener,
            config,
            std::future::pending(),
        ));
        addr
    }

    struct Client(TcpStream);

    impl Client {
        async fn connect(addr: SocketAddr) -> Client {
            Client(TcpStream::connect(addr).await.unwrap())
        }

        async fn send(&mut self, command: &str) {
            self.0
                .write_all(format!("{command}\r\n").as_bytes())
                .await
                .unwrap();
        }

        /// The next reply line, or the contents of a bulk string.
        async fn reply(&mut self) -> String {
            let mut line = Vec::new();
            let mut byte = [0u8; 1];
            while !line.ends_with(b"\r\n") {
                timeout(TIMEOUT, self.0.read_exact(&mut byte))
                    .await
                    .expect("timed out waiting for a reply")
                    .expect("connection closed");
                line.push(byte[0]);
            }
            let line = String::from_utf8(line).unwrap().trim_end().to_owned();
            let Some(len) = line
                .strip_prefix('$')
                .and_then(|len| len.parse::<usize>().ok())
            else {
                return line;
            };
            let mut body = vec![0u8; len + 2];
            timeout(TIMEOUT, self.0.read_exact(&mut body))
                .await
                .unwrap()
                .unwrap();
            body.truncate(len);
            String::from_utf8(body).unwrap()
        }

        async fn ask(&mut self, command: &str) -> String {
            self.send(command).await;
            self.reply().await
        }

        /// Builds a hash of `FIELDS` fields, a thousand per round trip.
        async fn build_hash(&mut self, key: &str) {
            for start in (0..FIELDS).step_by(1_000) {
                let mut request = String::new();
                for chunk in (start..start + 1_000).collect::<Vec<_>>().chunks(100) {
                    request.push_str(&format!("HSET {key}"));
                    for n in chunk {
                        request.push_str(&format!(" field-{n} value-{n}"));
                    }
                    request.push_str("\r\n");
                }
                self.0.write_all(request.as_bytes()).await.unwrap();
                for _ in 0..10 {
                    assert_eq!(self.reply().await, ":100");
                }
            }
        }

        async fn wait_for_save(&mut self) {
            let started = Instant::now();
            loop {
                let info = self.ask("INFO persistence").await;
                if info.contains("rdb_bgsave_in_progress:0") {
                    assert!(info.contains("rdb_last_bgsave_status:ok"), "{info}");
                    return;
                }
                assert!(started.elapsed() < TIMEOUT, "the save never finished");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }

    #[tokio::test]
    async fn a_write_to_a_large_hash_during_a_save_holds_up_only_its_own_client() {
        let dir = TempDir::new("held");
        let addr = start(&dir).await;
        let mut writer = Client::connect(addr).await;
        let mut other = Client::connect(addr).await;
        writer.build_hash("big").await;
        assert_eq!(other.ask("SET counter 0").await, "+OK");

        assert_eq!(other.ask("BGSAVE").await, "+Background saving started");
        // The write is sent first; it has to wait for the hash to be captured.
        let sent = Instant::now();
        writer.send("HSET big field-0 changed added yes").await;
        // Meanwhile another client is served as usual, many times over.
        let mut served = 0;
        let mut slowest = Duration::ZERO;
        let write_reply = loop {
            let asked = Instant::now();
            assert_eq!(other.ask("INCR counter").await, format!(":{}", served + 1));
            slowest = slowest.max(asked.elapsed());
            served += 1;
            // Has the writer been answered yet?
            let mut byte = [0u8; 1];
            match timeout(Duration::ZERO, writer.0.peek(&mut byte)).await {
                Ok(Ok(1)) => break writer.reply().await,
                _ => assert!(sent.elapsed() < TIMEOUT, "the write was never answered"),
            }
        };
        let held = sent.elapsed();
        assert_eq!(write_reply, ":1");
        // The other client got through a good many commands while the writer
        // waited, none of them slow. (Capturing 300,000 fields in one go takes
        // far longer than this in a test build.)
        assert!(served >= 5, "only {served} commands in {held:?}");
        assert!(
            slowest < held / 2,
            "another client waited {slowest:?} of the {held:?} the write was held"
        );
        // While it waited it did not count as a blocked client.
        assert!(other
            .ask("INFO clients")
            .await
            .contains("blocked_clients:0"));

        // The writer's next commands follow in order.
        assert_eq!(writer.ask("HGET big field-0").await, "changed");
        assert_eq!(writer.ask("HLEN big").await, format!(":{}", FIELDS + 1));
        other.wait_for_save().await;

        // The snapshot has the hash as it was when the save began.
        assert_eq!(other.ask("SET after save").await, "+OK");
        assert_eq!(other.ask("SHUTDOWN NOSAVE").await, "+OK");
        let addr = start(&dir).await;
        let mut fresh = Client::connect(addr).await;
        assert_eq!(fresh.ask("HLEN big").await, format!(":{FIELDS}"));
        assert_eq!(fresh.ask("HGET big field-0").await, "value-0");
        assert_eq!(fresh.ask("HEXISTS big added").await, ":0");
        assert_eq!(fresh.ask("GET counter").await, "0");
        assert_eq!(fresh.ask("EXISTS after").await, ":0");
    }

    #[tokio::test]
    async fn swapdb_during_a_save_waits_and_the_snapshot_keeps_the_databases_apart() {
        let dir = TempDir::new("swapdb");
        let addr = start(&dir).await;
        let mut writer = Client::connect(addr).await;
        let mut other = Client::connect(addr).await;
        writer.build_hash("big").await;
        assert_eq!(other.ask("SELECT 1").await, "+OK");
        assert_eq!(other.ask("SET small one").await, "+OK");

        assert_eq!(other.ask("BGSAVE").await, "+Background saving started");
        assert_eq!(other.ask("SWAPDB 0 1").await, "+OK");
        assert_eq!(other.ask("EXISTS small").await, ":0");
        assert_eq!(other.ask("HLEN big").await, format!(":{FIELDS}"));
        assert_eq!(writer.ask("GET small").await, "one");
        other.wait_for_save().await;

        // The snapshot has each database as it was when the save began.
        assert_eq!(other.ask("SHUTDOWN NOSAVE").await, "+OK");
        let addr = start(&dir).await;
        let mut fresh = Client::connect(addr).await;
        assert_eq!(fresh.ask("HLEN big").await, format!(":{FIELDS}"));
        assert_eq!(fresh.ask("EXISTS small").await, ":0");
        assert_eq!(fresh.ask("SELECT 1").await, "+OK");
        assert_eq!(fresh.ask("GET small").await, "one");
        assert_eq!(fresh.ask("EXISTS big").await, ":0");
    }

    #[tokio::test]
    async fn clients_held_back_are_let_go_when_the_save_is_abandoned() {
        let dir = TempDir::new("abandoned");
        let addr = start(&dir).await;
        let mut first = Client::connect(addr).await;
        let mut second = Client::connect(addr).await;
        let mut other = Client::connect(addr).await;
        first.build_hash("big").await;

        assert_eq!(other.ask("BGSAVE").await, "+Background saving started");
        // Two clients write to the hash, one after the other, each with a
        // second command behind the first.
        first.send("HSET big one 1\r\nHLEN big").await;
        // (Make sure the first one's write has arrived before the second's.)
        assert_eq!(other.ask("PING").await, "+PONG");
        second.send("HSET big two 2\r\nHLEN big").await;
        assert_eq!(other.ask("PING").await, "+PONG");
        // Flushing abandons the save. The held writes then run, in the order
        // they came, against the now empty database; each client's own commands
        // stay in order, and both writes are in before either count is taken.
        assert_eq!(other.ask("FLUSHALL").await, "+OK");
        assert_eq!(first.reply().await, ":1");
        assert_eq!(first.reply().await, ":2");
        assert_eq!(second.reply().await, ":1");
        assert_eq!(second.reply().await, ":2");
        other.wait_for_save().await;
        assert_eq!(other.ask("HLEN big").await, ":2");
    }
}
