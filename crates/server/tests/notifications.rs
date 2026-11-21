//! End-to-end tests for keyspace notifications (`notify-keyspace-events`):
//! what a subscriber receives, in what order relative to replies and other
//! messages, and the events nobody's command raises (the expiry cycle).

use std::net::SocketAddr;
use std::time::Duration;

use rexyra_server::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};

const TIMEOUT: Duration = Duration::from_secs(10);
/// Long enough for a command sent on one connection to have been executed
/// before the test carries on with another connection.
const SETTLE: Duration = Duration::from_millis(100);

async fn start_with(config: Config) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rexyra_server::serve(
        listener,
        config,
        std::future::pending(),
    ));
    addr
}

async fn start() -> SocketAddr {
    start_with(Config::default()).await
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

    /// Asserts that exactly these bytes arrive next.
    async fn expect(&mut self, expected: &str) {
        let mut got = vec![0u8; expected.len()];
        timeout(TIMEOUT, self.0.read_exact(&mut got))
            .await
            .expect("timed out waiting for data")
            .expect("connection closed early");
        assert_eq!(String::from_utf8_lossy(&got), expected);
    }

    async fn check(&mut self, command: &str, expected: &str) {
        self.send(command).await;
        self.expect(expected).await;
    }

    /// Asserts that the server has sent nothing more.
    async fn expect_silence(&mut self) {
        let mut byte = [0u8; 1];
        let read = timeout(SETTLE, self.0.read(&mut byte)).await;
        assert!(read.is_err(), "unexpected data: {read:?} {byte:?}");
    }
}

fn bulk(text: &str) -> String {
    format!("${}\r\n{text}\r\n", text.len())
}

/// A message received through `PSUBSCRIBE pattern`, in RESP2.
fn pmessage(pattern: &str, channel: &str, text: &str) -> String {
    format!(
        "*4\r\n$8\r\npmessage\r\n{}{}{}",
        bulk(pattern),
        bulk(channel),
        bulk(text)
    )
}

/// Both notifications of one event, as `PSUBSCRIBE __key*@*__:*` gets them.
fn both(db: usize, key: &str, event: &str) -> String {
    let pattern = "__key*@*__:*";
    pmessage(pattern, &format!("__keyspace@{db}__:{key}"), event)
        + &pmessage(pattern, &format!("__keyevent@{db}__:{event}"), key)
}

/// A subscriber to every notification.
async fn listener(addr: SocketAddr) -> Client {
    let mut client = Client::connect(addr).await;
    client
        .check(
            "PSUBSCRIBE __key*@*__:*",
            "*3\r\n$10\r\npsubscribe\r\n$12\r\n__key*@*__:*\r\n:1\r\n",
        )
        .await;
    client
}

#[tokio::test]
async fn nothing_is_published_until_the_setting_asks_for_it() {
    let addr = start().await;
    let mut events = listener(addr).await;
    let mut client = Client::connect(addr).await;
    client
        .check(
            "CONFIG GET notify-keyspace-events",
            "*2\r\n$22\r\nnotify-keyspace-events\r\n$0\r\n\r\n",
        )
        .await;
    client.check("SET k v", "+OK\r\n").await;
    events.expect_silence().await;

    client
        .check("CONFIG SET notify-keyspace-events KEA", "+OK\r\n")
        .await;
    client.check("SET k v EX 100", "+OK\r\n").await;
    client.check("DEL k", ":1\r\n").await;
    events
        .expect(&(both(0, "k", "set") + &both(0, "k", "expire") + &both(0, "k", "del")))
        .await;
    client
        .check("CONFIG SET notify-keyspace-events \"\"", "+OK\r\n")
        .await;
    client.check("SET k v", "+OK\r\n").await;
    events.expect_silence().await;
}

#[tokio::test]
async fn the_setting_from_the_configuration_applies_from_the_start() {
    let config = Config {
        notify_keyspace_events: rexyra_commands::notify::parse(b"Kg").unwrap(),
        ..Config::default()
    };
    let addr = start_with(config).await;
    let mut events = listener(addr).await;
    let mut client = Client::connect(addr).await;
    client
        .check(
            "CONFIG GET notify-keyspace-events",
            "*2\r\n$22\r\nnotify-keyspace-events\r\n$2\r\ngK\r\n",
        )
        .await;
    // `$` is not enabled, so SET says nothing; `g` is, so DEL does.
    client.check("SELECT 4", "+OK\r\n").await;
    client.check("SET k v", "+OK\r\n").await;
    client.check("DEL k", ":1\r\n").await;
    events
        .expect(&pmessage("__key*@*__:*", "__keyspace@4__:k", "del"))
        .await;
    events.expect_silence().await;
}

#[tokio::test]
async fn a_served_blocked_client_reports_its_pop_after_the_push() {
    let addr = start().await;
    let mut events = listener(addr).await;
    let mut client = Client::connect(addr).await;
    let mut blocked = Client::connect(addr).await;
    client
        .check("CONFIG SET notify-keyspace-events KEA", "+OK\r\n")
        .await;
    blocked.send("BLPOP q 0").await;
    sleep(SETTLE).await;
    client.check("LPUSH q v", ":1\r\n").await;
    blocked.expect("*2\r\n$1\r\nq\r\n$1\r\nv\r\n").await;
    events
        .expect(&(both(0, "q", "lpush") + &both(0, "q", "lpop") + &both(0, "q", "del")))
        .await;
}

#[tokio::test]
async fn a_resp3_client_gets_its_own_events_after_the_reply() {
    let addr = start().await;
    let mut client = Client::connect(addr).await;
    client
        .check("CONFIG SET notify-keyspace-events KEA", "+OK\r\n")
        .await;
    client.send("HELLO 3").await;
    sleep(SETTLE).await;
    let mut greeting = vec![0u8; 4096];
    let _ = client.0.read(&mut greeting).await.unwrap();
    client
        .check(
            "SUBSCRIBE __keyspace@0__:k",
            ">3\r\n$9\r\nsubscribe\r\n$16\r\n__keyspace@0__:k\r\n:1\r\n",
        )
        .await;
    let push = |event: &str| {
        format!(
            ">3\r\n$7\r\nmessage\r\n$16\r\n__keyspace@0__:k\r\n{}",
            bulk(event)
        )
    };
    // As in Redis, a message for the client running the command comes
    // after that command's reply, and after the whole of EXEC's.
    client.send("SET k v\r\nGET k").await;
    client
        .expect(&format!("+OK\r\n{}$1\r\nv\r\n", push("set")))
        .await;
    client.send("MULTI\r\nINCR k\r\nDEL k\r\nEXEC").await;
    client
        .expect(&format!(
            "+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n-ERR value is not an integer or out of range\r\n:1\r\n{}",
            push("del")
        ))
        .await;
}

#[tokio::test]
async fn keys_removed_by_the_expiry_cycle_are_announced() {
    let addr = start().await;
    let mut events = listener(addr).await;
    let mut client = Client::connect(addr).await;
    client
        .check("CONFIG SET notify-keyspace-events Ex", "+OK\r\n")
        .await;
    client.check("SET k v PX 50", "+OK\r\n").await;
    // Nobody touches the key: the periodic cycle deletes it.
    events
        .expect(&pmessage("__key*@*__:*", "__keyevent@0__:expired", "k"))
        .await;
}

#[tokio::test]
async fn without_the_expiry_cycle_a_lookup_finds_the_key_expired() {
    let config = Config {
        enable_debug_command: rexyra_commands::DebugCommand::Yes,
        ..Config::default()
    };
    let addr = start_with(config).await;
    let mut events = listener(addr).await;
    let mut client = Client::connect(addr).await;
    client
        .check("CONFIG SET notify-keyspace-events KExm", "+OK\r\n")
        .await;
    client.check("DEBUG SET-ACTIVE-EXPIRE 0", "+OK\r\n").await;
    client.check("SET k v PX 20", "+OK\r\n").await;
    sleep(Duration::from_millis(300)).await;
    events.expect_silence().await;
    client.check("GET k", "$-1\r\n").await;
    events
        .expect(&(both(0, "k", "expired") + &both(0, "k", "keymiss")))
        .await;
    client.check("DEBUG SET-ACTIVE-EXPIRE 1", "+OK\r\n").await;
}

#[tokio::test]
async fn eviction_is_announced() {
    let addr = start().await;
    let mut events = listener(addr).await;
    let mut client = Client::connect(addr).await;
    client
        .check("CONFIG SET notify-keyspace-events Ee", "+OK\r\n")
        .await;
    client.check("SET victim v EX 100", "+OK\r\n").await;
    client
        .check("CONFIG SET maxmemory-policy volatile-ttl", "+OK\r\n")
        .await;
    client.check("CONFIG SET maxmemory 1", "+OK\r\n").await;
    // Room is made before the next command runs.
    client.check("ECHO x", "$1\r\nx\r\n").await;
    events
        .expect(&pmessage(
            "__key*@*__:*",
            "__keyevent@0__:evicted",
            "victim",
        ))
        .await;
    client.check("CONFIG SET maxmemory 0", "+OK\r\n").await;
}

#[tokio::test]
async fn notifications_respect_the_channel_rules_of_the_subscriber() {
    let addr = start().await;
    let mut admin = Client::connect(addr).await;
    admin
        .check("CONFIG SET notify-keyspace-events KEA", "+OK\r\n")
        .await;
    admin
        .check(
            "ACL SETUSER watcher on >pw +@all ~* resetchannels &__keyspace@0__:*",
            "+OK\r\n",
        )
        .await;
    let mut watcher = Client::connect(addr).await;
    watcher.check("AUTH watcher pw", "+OK\r\n").await;
    watcher
        .check(
            "PSUBSCRIBE __keyevent@0__:*",
            "-NOPERM No permissions to access a channel\r\n",
        )
        .await;
    watcher
        .check(
            "PSUBSCRIBE __keyspace@0__:*",
            "*3\r\n$10\r\npsubscribe\r\n$16\r\n__keyspace@0__:*\r\n:1\r\n",
        )
        .await;
    admin.check("SET k v", "+OK\r\n").await;
    watcher
        .expect(&pmessage("__keyspace@0__:*", "__keyspace@0__:k", "set"))
        .await;
    watcher.expect_silence().await;
}
