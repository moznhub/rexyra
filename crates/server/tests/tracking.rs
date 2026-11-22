//! End-to-end tests for client-side caching (`CLIENT TRACKING`): real
//! connections, and the bytes each one receives, in order.

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

async fn start() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rexyra_server::serve(
        listener,
        Config::default(),
        std::future::pending(),
    ));
    addr
}

struct Client(TcpStream);

impl Client {
    async fn connect(addr: SocketAddr) -> Client {
        Client(TcpStream::connect(addr).await.unwrap())
    }

    /// A connection that has switched to RESP3.
    async fn resp3(addr: SocketAddr) -> Client {
        let mut client = Client::connect(addr).await;
        client.send("HELLO 3").await;
        client.read_until("$7\r\nmodules\r\n*0\r\n").await;
        client
    }

    async fn send(&mut self, commands: &str) {
        self.0
            .write_all(format!("{commands}\r\n").as_bytes())
            .await
            .unwrap();
    }

    async fn read(&mut self, len: usize) -> String {
        let mut got = vec![0u8; len];
        timeout(TIMEOUT, self.0.read_exact(&mut got))
            .await
            .expect("timed out waiting for data")
            .expect("connection closed early");
        String::from_utf8_lossy(&got).into_owned()
    }

    /// Asserts that exactly these bytes arrive next.
    async fn expect(&mut self, expected: &str) {
        assert_eq!(self.read(expected.len()).await, expected);
    }

    async fn check(&mut self, command: &str, expected: &str) {
        self.send(command).await;
        self.expect(expected).await;
    }

    async fn read_until(&mut self, end: &str) -> String {
        let mut got = String::new();
        while !got.ends_with(end) {
            got.push_str(&self.read(1).await);
        }
        got
    }

    /// Asserts that the server has sent nothing more.
    async fn expect_silence(&mut self) {
        let mut byte = [0u8; 1];
        let read = timeout(SETTLE, self.0.read(&mut byte)).await;
        assert!(read.is_err(), "unexpected data: {read:?} {byte:?}");
    }

    async fn id(&mut self) -> u64 {
        self.send("CLIENT ID").await;
        let line = self.read_until("\r\n").await;
        line[1..line.len() - 2].parse().unwrap()
    }
}

/// A RESP3 invalidation of `keys` (`None`: of everything).
fn invalidate(keys: Option<&[&str]>) -> String {
    let payload = match keys {
        None => "_\r\n".to_owned(),
        Some(keys) => {
            let mut text = format!("*{}\r\n", keys.len());
            for key in keys {
                text.push_str(&format!("${}\r\n{key}\r\n", key.len()));
            }
            text
        }
    };
    format!(">2\r\n$10\r\ninvalidate\r\n{payload}")
}

#[tokio::test]
async fn a_resp3_client_is_pushed_an_invalidation_after_any_reply() {
    let addr = start().await;
    let mut reader = Client::resp3(addr).await;
    let mut writer = Client::connect(addr).await;
    reader.check("CLIENT TRACKING ON", "+OK\r\n").await;
    reader.check("GET k", "_\r\n").await;
    writer.check("SET k 1", "+OK\r\n").await;
    reader.expect(&invalidate(Some(&["k"]))).await;
    writer.check("SET k 2", "+OK\r\n").await;
    reader.expect_silence().await;
    // Its own write: the reply, then the message, before the next reply.
    reader.send("GET k\r\nSET k 3\r\nPING").await;
    reader
        .expect(&format!(
            "$1\r\n2\r\n+OK\r\n{}+PONG\r\n",
            invalidate(Some(&["k"]))
        ))
        .await;
    // A flush: everything.
    reader.check("GET k", "$1\r\n3\r\n").await;
    writer.check("FLUSHALL", "+OK\r\n").await;
    reader.expect(&invalidate(None)).await;
}

#[tokio::test]
async fn a_resp2_client_redirects_to_a_pubsub_connection() {
    let addr = start().await;
    let mut listener = Client::connect(addr).await;
    let mut tracker = Client::connect(addr).await;
    let mut writer = Client::connect(addr).await;
    let id = listener.id().await;
    listener
        .check(
            "SUBSCRIBE __redis__:invalidate",
            "*3\r\n$9\r\nsubscribe\r\n$20\r\n__redis__:invalidate\r\n:1\r\n",
        )
        .await;
    tracker
        .check(&format!("CLIENT TRACKING ON REDIRECT {id}"), "+OK\r\n")
        .await;
    tracker.check("GET k", "$-1\r\n").await;
    writer.check("SET k 1", "+OK\r\n").await;
    listener
        .expect("*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n$1\r\nk\r\n")
        .await;
    tracker.expect_silence().await;
    // When the target goes, the tracker (RESP2) cannot be told, but it
    // shows.
    drop(listener);
    sleep(SETTLE).await;
    tracker.check("GET k", "$1\r\n1\r\n").await;
    writer.check("SET k 2", "+OK\r\n").await;
    tracker.send("CLIENT INFO").await;
    let line = tracker.read_until("\n\r\n").await;
    assert!(
        line.contains(" flags=tR ") && line.contains(&format!(" redir={id} ")),
        "{line}"
    );
}

#[tokio::test]
async fn a_resp3_tracker_hears_that_its_redirect_target_has_gone() {
    let addr = start().await;
    let mut target = Client::resp3(addr).await;
    let mut tracker = Client::resp3(addr).await;
    let mut writer = Client::connect(addr).await;
    let id = target.id().await;
    tracker
        .check(&format!("CLIENT TRACKING ON REDIRECT {id}"), "+OK\r\n")
        .await;
    tracker.check("GET k", "_\r\n").await;
    writer.check("SET k 1", "+OK\r\n").await;
    target.expect(&invalidate(Some(&["k"]))).await;
    writer
        .check(&format!("CLIENT KILL ID {id}"), ":1\r\n")
        .await;
    sleep(SETTLE).await;
    tracker.check("GET k", "$1\r\n1\r\n").await;
    writer.check("SET k 2", "+OK\r\n").await;
    tracker
        .expect(&format!(">2\r\n$21\r\ntracking-redir-broken\r\n:{id}\r\n"))
        .await;
}

#[tokio::test]
async fn keys_expired_by_the_cycle_are_invalidated() {
    let addr = start().await;
    let mut reader = Client::resp3(addr).await;
    let mut writer = Client::connect(addr).await;
    reader.check("CLIENT TRACKING ON", "+OK\r\n").await;
    writer.check("SET k v PX 150", "+OK\r\n").await;
    reader.check("GET k", "$1\r\nv\r\n").await;
    // Nobody reads it again: the periodic cycle finds it.
    reader.expect(&invalidate(Some(&["k"]))).await;
    reader.expect_silence().await;
}

#[tokio::test]
async fn the_table_is_cut_down_to_its_limit() {
    let addr = start().await;
    let mut reader = Client::resp3(addr).await;
    let mut admin = Client::connect(addr).await;
    reader.check("CLIENT TRACKING ON", "+OK\r\n").await;
    reader.check("MGET k1 k2 k3", "*3\r\n_\r\n_\r\n_\r\n").await;
    admin
        .check("CONFIG SET tracking-table-max-keys 1", "+OK\r\n")
        .await;
    // Two of the three, picked at random, each in its own message.
    let one = invalidate(Some(&["k1"]));
    let got = reader.read(2 * one.len()).await;
    let dropped: Vec<&str> = ["k1", "k2", "k3"]
        .into_iter()
        .filter(|key| got.contains(&invalidate(Some(&[key]))))
        .collect();
    assert_eq!(dropped.len(), 2, "{got:?}");
    reader.expect_silence().await;
    admin.send("INFO stats").await;
    let info = admin.read_until("\r\n\r\n").await;
    assert!(info.contains("tracking_total_keys:1\r\n"), "{info}");
}

#[tokio::test]
async fn broadcasting_sends_one_message_per_prefix_for_a_whole_pipeline() {
    let addr = start().await;
    let mut listener = Client::resp3(addr).await;
    let mut writer = Client::connect(addr).await;
    listener
        .check("CLIENT TRACKING ON BCAST PREFIX a PREFIX b", "+OK\r\n")
        .await;
    writer
        .check(
            "SET a2 1\r\nSET b1 1\r\nSET a1 1\r\nSET c1 1\r\nSET a2 2",
            "+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n",
        )
        .await;
    listener
        .expect(&format!(
            "{}{}",
            invalidate(Some(&["a1", "a2"])),
            invalidate(Some(&["b1"]))
        ))
        .await;
    listener.expect_silence().await;
    // A transaction is batched the same way.
    writer
        .check(
            "MULTI\r\nSET a4 1\r\nSET a3 1\r\nEXEC",
            "+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n+OK\r\n",
        )
        .await;
    listener.expect(&invalidate(Some(&["a3", "a4"]))).await;
    // Its own writes come after its reply.
    listener
        .check("SET b2 1", &format!("+OK\r\n{}", invalidate(Some(&["b2"]))))
        .await;
}

#[tokio::test]
async fn optin_tracks_the_command_after_caching_yes_in_a_pipeline() {
    let addr = start().await;
    let mut reader = Client::resp3(addr).await;
    let mut writer = Client::connect(addr).await;
    reader.check("CLIENT TRACKING ON OPTIN", "+OK\r\n").await;
    reader
        .check("CLIENT CACHING YES\r\nGET a\r\nGET b", "+OK\r\n_\r\n_\r\n")
        .await;
    writer.check("MSET a 1 b 1", "+OK\r\n").await;
    reader.expect(&invalidate(Some(&["a"]))).await;
    reader.expect_silence().await;
}
