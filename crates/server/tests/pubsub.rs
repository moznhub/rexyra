//! End-to-end tests for Pub/Sub: several real connections, the bytes each
//! one receives, and the order they arrive in.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

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

fn message(channel: &str, text: &str) -> String {
    format!(
        "*3\r\n$7\r\nmessage\r\n${}\r\n{channel}\r\n${}\r\n{text}\r\n",
        channel.len(),
        text.len()
    )
}

#[tokio::test]
async fn a_message_reaches_every_subscriber() {
    let addr = start().await;
    let mut one = Client::connect(addr).await;
    let mut two = Client::connect(addr).await;
    let mut publisher = Client::connect(addr).await;

    one.check(
        "SUBSCRIBE news",
        "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n",
    )
    .await;
    two.check(
        "PSUBSCRIBE n*",
        "*3\r\n$10\r\npsubscribe\r\n$2\r\nn*\r\n:1\r\n",
    )
    .await;
    publisher.check("PUBLISH news hello", ":2\r\n").await;
    one.expect(&message("news", "hello")).await;
    two.expect("*4\r\n$8\r\npmessage\r\n$2\r\nn*\r\n$4\r\nnews\r\n$5\r\nhello\r\n")
        .await;
    publisher.check("PUBLISH sport goal", ":0\r\n").await;
    one.expect_silence().await;
}

#[tokio::test]
async fn messages_never_overtake_the_replies_before_them() {
    let addr = start().await;
    let mut subscriber = Client::connect(addr).await;
    let mut publisher = Client::connect(addr).await;

    // Publish as fast as possible while the subscription is being made:
    // whatever arrives, the confirmation comes first, then PING's answer,
    // then messages, each whole and in the order published.
    let flood = tokio::spawn(async move {
        let mut sent = 0;
        while sent < 200 {
            publisher.send(&format!("PUBLISH c {sent}")).await;
            let mut reply = [0u8; 4];
            publisher.0.read_exact(&mut reply).await.unwrap();
            if &reply == b":1\r\n" {
                sent += 1;
            }
        }
    });
    subscriber.send("SUBSCRIBE c\r\nPING").await;
    subscriber
        .expect("*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n*2\r\n$4\r\npong\r\n$0\r\n\r\n")
        .await;
    flood.await.unwrap();
    // The messages counted as received are 0..200, in order; earlier ones
    // were published before the subscription and went nowhere.
    let mut expected = String::new();
    for n in 0..200 {
        expected.push_str(&message("c", &n.to_string()));
    }
    subscriber.expect(&expected).await;
    subscriber.expect_silence().await;
}

#[tokio::test]
async fn resp2_subscribers_may_only_manage_subscriptions() {
    let addr = start().await;
    let mut c = Client::connect(addr).await;
    c.check("SUBSCRIBE a", "*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n")
        .await;
    c.check(
        "GET k",
        "-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n",
    )
    .await;
    c.check("PING hi", "*2\r\n$4\r\npong\r\n$2\r\nhi\r\n").await;
    c.check(
        "UNSUBSCRIBE",
        "*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:0\r\n",
    )
    .await;
    c.check("GET k", "$-1\r\n").await;
}

#[tokio::test]
async fn resp3_subscribers_get_push_frames_and_keep_every_command() {
    let addr = start().await;
    let mut c = Client::connect(addr).await;
    let mut publisher = Client::connect(addr).await;
    c.send("HELLO 3").await;
    sleep(SETTLE).await;
    let mut hello = vec![0u8; 4096];
    let _ = c.0.read(&mut hello).await.unwrap();

    c.check("SUBSCRIBE a", ">3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n")
        .await;
    publisher.check("PUBLISH a x", ":1\r\n").await;
    c.expect(">3\r\n$7\r\nmessage\r\n$1\r\na\r\n$1\r\nx\r\n")
        .await;
    c.check("SET k v", "+OK\r\n").await;

    // Publishing to itself: the reply, then the message.
    c.check(
        "PUBLISH a self",
        ":1\r\n>3\r\n$7\r\nmessage\r\n$1\r\na\r\n$4\r\nself\r\n",
    )
    .await;
}

#[tokio::test]
async fn a_blocked_resp3_client_still_receives_messages() {
    let addr = start().await;
    let mut c = Client::connect(addr).await;
    let mut other = Client::connect(addr).await;
    c.send("HELLO 3").await;
    sleep(SETTLE).await;
    let mut hello = vec![0u8; 4096];
    let _ = c.0.read(&mut hello).await.unwrap();

    c.send("SUBSCRIBE a\r\nBLPOP q 0").await;
    c.expect(">3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n").await;
    c.expect_silence().await;
    other.check("PUBLISH a while-blocked", ":1\r\n").await;
    c.expect(">3\r\n$7\r\nmessage\r\n$1\r\na\r\n$13\r\nwhile-blocked\r\n")
        .await;
    other.check("RPUSH q item", ":1\r\n").await;
    c.expect("*2\r\n$1\r\nq\r\n$4\r\nitem\r\n").await;
}

#[tokio::test]
async fn a_subscriber_that_leaves_is_forgotten() {
    let addr = start().await;
    let mut admin = Client::connect(addr).await;
    {
        let mut c = Client::connect(addr).await;
        c.check(
            "SUBSCRIBE a b",
            "*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n",
        )
        .await;
        admin
            .check("PUBSUB NUMSUB a", "*2\r\n$1\r\na\r\n:1\r\n")
            .await;
        let list = {
            admin.send("CLIENT LIST TYPE pubsub").await;
            sleep(SETTLE).await;
            let mut buf = vec![0u8; 4096];
            let n = admin.0.read(&mut buf).await.unwrap();
            String::from_utf8_lossy(&buf[..n]).into_owned()
        };
        assert!(list.contains(" flags=P "), "{list}");
        assert!(list.contains(" sub=2 psub=0 ssub=0 "), "{list}");
    }
    sleep(SETTLE).await;
    admin
        .check("PUBSUB NUMSUB a", "*2\r\n$1\r\na\r\n:0\r\n")
        .await;
    admin.check("PUBLISH a x", ":0\r\n").await;
}

#[tokio::test]
async fn a_subscriber_that_does_not_read_is_dropped_at_its_limit() {
    let addr = start().await;
    let mut slow = Client::connect(addr).await;
    let mut publisher = Client::connect(addr).await;
    slow.check("SUBSCRIBE c", "*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n")
        .await;

    // 1 MB messages, which the subscriber never reads: once the socket's
    // buffers are full they pile up in the server, until the 32 MB limit.
    let payload = "x".repeat(1024 * 1024);
    let request = format!(
        "*3\r\n$7\r\nPUBLISH\r\n$1\r\nc\r\n${}\r\n{payload}\r\n",
        payload.len()
    );
    let started = Instant::now();
    let mut dropped = false;
    for _ in 0..200 {
        publisher.0.write_all(request.as_bytes()).await.unwrap();
        let mut reply = [0u8; 4];
        publisher.0.read_exact(&mut reply).await.unwrap();
        if &reply == b":0\r\n" {
            dropped = true;
            break;
        }
    }
    assert!(dropped, "the slow subscriber was never disconnected");
    // The publisher was not held up meanwhile.
    assert!(started.elapsed() < TIMEOUT);
    publisher
        .check("PUBSUB NUMSUB c", "*2\r\n$1\r\nc\r\n:0\r\n")
        .await;
}
