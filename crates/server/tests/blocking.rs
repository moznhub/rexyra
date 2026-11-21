//! End-to-end tests for the blocking commands: several real connections,
//! real clock.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use rexyra_server::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};

const TIMEOUT: Duration = Duration::from_secs(5);
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
            .expect("timed out waiting for reply")
            .expect("connection closed early");
        assert_eq!(String::from_utf8_lossy(&got), expected);
    }

    async fn check(&mut self, command: &str, expected: &str) {
        self.send(command).await;
        self.expect(expected).await;
    }

    /// Asserts that the server has sent nothing (the client is still blocked).
    async fn expect_silence(&mut self) {
        let mut byte = [0u8; 1];
        let read = timeout(SETTLE, self.0.read(&mut byte)).await;
        assert!(read.is_err(), "unexpected data: {read:?} {byte:?}");
    }

    /// Sends a command that is expected to block, and waits until it has.
    async fn block(&mut self, command: &str) {
        self.send(command).await;
        self.expect_silence().await;
    }
}

#[tokio::test]
async fn blpop_waits_for_a_push_from_another_client() {
    let addr = start().await;
    let mut waiter = Client::connect(addr).await;
    let mut pusher = Client::connect(addr).await;

    waiter.block("BLPOP jobs 0").await;
    pusher.check("RPUSH jobs a b", ":2\r\n").await;
    waiter.expect("*2\r\n$4\r\njobs\r\n$1\r\na\r\n").await;
    // Only one element was handed over.
    pusher.check("LRANGE jobs 0 -1", "*1\r\n$1\r\nb\r\n").await;
    // The waiter is an ordinary client again.
    waiter.check("PING", "+PONG\r\n").await;
}

#[tokio::test]
async fn waiters_are_served_in_arrival_order() {
    let addr = start().await;
    let mut first = Client::connect(addr).await;
    let mut second = Client::connect(addr).await;
    let mut third = Client::connect(addr).await;
    let mut pusher = Client::connect(addr).await;

    first.block("BRPOP q 0").await;
    second.block("BLPOP other q 0").await;
    third.block("BLPOP q 0").await;

    pusher.check("RPUSH q 1 2", ":2\r\n").await;
    first.expect("*2\r\n$1\r\nq\r\n$1\r\n2\r\n").await;
    second.expect("*2\r\n$1\r\nq\r\n$1\r\n1\r\n").await;
    third.expect_silence().await;
    pusher.check("EXISTS q", ":0\r\n").await;

    pusher.check("LPUSH q 3", ":1\r\n").await;
    third.expect("*2\r\n$1\r\nq\r\n$1\r\n3\r\n").await;
}

#[tokio::test]
async fn timeout_replies_null_in_both_protocols() {
    let addr = start().await;
    let mut c = Client::connect(addr).await;

    let started = Instant::now();
    c.check("BLPOP nothing 0.2", "*-1\r\n").await;
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(200), "{waited:?}");
    assert!(waited < Duration::from_millis(1500), "{waited:?}");

    c.check("BRPOPLPUSH nothing dst 0.05", "*-1\r\n").await;
    c.check("BZPOPMIN nothing 0.05", "*-1\r\n").await;
    c.check("BLMPOP 0.05 1 nothing LEFT", "*-1\r\n").await;

    c.send("HELLO 3").await;
    let mut hello = [0u8; 4096];
    timeout(TIMEOUT, c.0.read(&mut hello))
        .await
        .unwrap()
        .unwrap();
    c.check("BLPOP nothing 0.05", "_\r\n").await;
    c.check("BZMPOP 0.05 1 nothing MIN", "_\r\n").await;
}

#[tokio::test]
async fn a_timed_out_client_no_longer_takes_elements() {
    let addr = start().await;
    let mut waiter = Client::connect(addr).await;
    let mut pusher = Client::connect(addr).await;

    waiter.check("BLPOP q 0.05", "*-1\r\n").await;
    pusher.check("RPUSH q a", ":1\r\n").await;
    waiter.expect_silence().await;
    pusher.check("LLEN q", ":1\r\n").await;
}

#[tokio::test]
async fn pipelined_commands_after_a_blocking_one_wait_their_turn() {
    let addr = start().await;
    let mut waiter = Client::connect(addr).await;
    let mut pusher = Client::connect(addr).await;

    waiter
        .send("SET before 1\r\nBLPOP q 0\r\nLLEN q\r\nBLPOP q2 0\r\nPING")
        .await;
    // Replies before the blocking command are sent while it waits, as in
    // Redis; nothing after it has run yet.
    waiter.expect("+OK\r\n").await;
    waiter.expect_silence().await;
    pusher.check("GET before", "$1\r\n1\r\n").await;

    // The pusher's own pipeline sees the element already taken.
    pusher.check("RPUSH q a\r\nLLEN q", ":1\r\n:0\r\n").await;
    // The waiter has gone on to the next blocking command.
    waiter.expect("*2\r\n$1\r\nq\r\n$1\r\na\r\n:0\r\n").await;
    waiter.expect_silence().await;

    pusher.check("RPUSH q2 b", ":1\r\n").await;
    waiter
        .expect("*2\r\n$2\r\nq2\r\n$1\r\nb\r\n+PONG\r\n")
        .await;
}

#[tokio::test]
async fn commands_sent_while_blocked_are_answered_afterwards() {
    let addr = start().await;
    let mut waiter = Client::connect(addr).await;
    let mut pusher = Client::connect(addr).await;

    waiter.block("BLPOP q 0").await;
    // These arrive while the server is still holding the BLPOP.
    waiter.block("PING").await;
    waiter.block("ECHO second").await;

    pusher.check("RPUSH q a", ":1\r\n").await;
    waiter
        .expect("*2\r\n$1\r\nq\r\n$1\r\na\r\n+PONG\r\n$6\r\nsecond\r\n")
        .await;
    waiter.check("PING", "+PONG\r\n").await;
}

#[tokio::test]
async fn blmove_feeds_a_client_waiting_on_its_destination() {
    let addr = start().await;
    let mut mover = Client::connect(addr).await;
    let mut consumer = Client::connect(addr).await;
    let mut pusher = Client::connect(addr).await;

    mover.block("BLMOVE src dst LEFT RIGHT 0").await;
    consumer.block("BLPOP dst 0").await;

    pusher.check("RPUSH src x", ":1\r\n").await;
    mover.expect("$1\r\nx\r\n").await;
    consumer.expect("*2\r\n$3\r\ndst\r\n$1\r\nx\r\n").await;
    pusher.check("EXISTS src dst", ":0\r\n").await;
}

#[tokio::test]
async fn bzpop_waits_for_zadd() {
    let addr = start().await;
    let mut low = Client::connect(addr).await;
    let mut high = Client::connect(addr).await;
    let mut multi = Client::connect(addr).await;
    let mut writer = Client::connect(addr).await;

    low.block("BZPOPMIN z 0").await;
    high.block("BZPOPMAX z 0").await;
    multi.block("BZMPOP 0 2 y z MIN COUNT 10").await;

    writer.check("ZADD z 1 a 2.5 b 3 c 4 d", ":4\r\n").await;
    low.expect("*3\r\n$1\r\nz\r\n$1\r\na\r\n$1\r\n1\r\n").await;
    high.expect("*3\r\n$1\r\nz\r\n$1\r\nd\r\n$1\r\n4\r\n").await;
    multi
        .expect("*2\r\n$1\r\nz\r\n*2\r\n*2\r\n$1\r\nb\r\n$3\r\n2.5\r\n*2\r\n$1\r\nc\r\n$1\r\n3\r\n")
        .await;
    writer.check("EXISTS z", ":0\r\n").await;
}

#[tokio::test]
async fn a_key_of_another_type_does_not_wake_the_waiter() {
    let addr = start().await;
    let mut list_waiter = Client::connect(addr).await;
    let mut zset_waiter = Client::connect(addr).await;
    let mut writer = Client::connect(addr).await;

    zset_waiter.block("BZPOPMIN k 0").await;
    list_waiter.block("BLPOP k 0").await;

    writer.check("SET k string", "+OK\r\n").await;
    list_waiter.expect_silence().await;
    zset_waiter.expect_silence().await;

    // The list waiter is served although the sorted-set waiter came first.
    writer.check("DEL k", ":1\r\n").await;
    writer.check("RPUSH k a", ":1\r\n").await;
    list_waiter.expect("*2\r\n$1\r\nk\r\n$1\r\na\r\n").await;
    zset_waiter.expect_silence().await;

    writer.check("ZADD k 7 m", ":1\r\n").await;
    zset_waiter
        .expect("*3\r\n$1\r\nk\r\n$1\r\nm\r\n$1\r\n7\r\n")
        .await;
}

#[tokio::test]
async fn keys_created_by_other_commands_wake_waiters() {
    let addr = start().await;
    let mut waiter = Client::connect(addr).await;
    let mut writer = Client::connect(addr).await;

    writer.check("RPUSH src a", ":1\r\n").await;
    waiter.block("BLPOP dst 0").await;
    writer.check("RENAME src dst", "+OK\r\n").await;
    waiter.expect("*2\r\n$3\r\ndst\r\n$1\r\na\r\n").await;

    writer.check("RPUSH src b", ":1\r\n").await;
    waiter.block("BLPOP dst 0").await;
    writer.check("COPY src dst", ":1\r\n").await;
    waiter.expect("*2\r\n$3\r\ndst\r\n$1\r\nb\r\n").await;
}

#[tokio::test]
async fn waiting_is_scoped_to_the_selected_database() {
    let addr = start().await;
    let mut waiter = Client::connect(addr).await;
    let mut writer = Client::connect(addr).await;

    waiter.check("SELECT 2", "+OK\r\n").await;
    waiter.block("BLPOP q 0").await;
    writer.check("RPUSH q zero", ":1\r\n").await;
    waiter.expect_silence().await;

    writer.check("SELECT 2", "+OK\r\n").await;
    writer.check("RPUSH q two", ":1\r\n").await;
    waiter.expect("*2\r\n$1\r\nq\r\n$3\r\ntwo\r\n").await;
}

#[tokio::test]
async fn a_disconnected_waiter_does_not_swallow_elements() {
    let addr = start().await;
    let mut gone = Client::connect(addr).await;
    let mut waiter = Client::connect(addr).await;
    let mut pusher = Client::connect(addr).await;

    gone.block("BLPOP q 0").await;
    waiter.block("BLPOP q 0").await;
    drop(gone);
    sleep(SETTLE).await;

    pusher.check("RPUSH q a", ":1\r\n").await;
    waiter.expect("*2\r\n$1\r\nq\r\n$1\r\na\r\n").await;
}

#[tokio::test]
async fn a_half_closed_pipeline_still_gets_its_replies() {
    let addr = start().await;
    let mut c = Client::connect(addr).await;
    c.send("SET k v\r\nGET k").await;
    // Close our sending side only, as `printf ... | nc` does.
    c.0.shutdown().await.unwrap();
    c.expect("+OK\r\n$1\r\nv\r\n").await;
}

#[tokio::test]
async fn many_waiters_and_pushers_lose_nothing() {
    const WAITERS: usize = 20;
    const PER_WAITER: usize = 25;
    let addr = start().await;

    let waiters: Vec<_> = (0..WAITERS)
        .map(|_| {
            tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                let mut got = Vec::new();
                for _ in 0..PER_WAITER {
                    c.send("BLPOP q 0").await;
                    // "*2\r\n$1\r\nq\r\n$<len>\r\n<digits>\r\n"
                    let mut reply = Vec::new();
                    let mut byte = [0u8; 1];
                    while reply.iter().filter(|&&b| b == b'\n').count() < 5 {
                        timeout(TIMEOUT, c.0.read_exact(&mut byte))
                            .await
                            .expect("waiter starved")
                            .unwrap();
                        reply.push(byte[0]);
                    }
                    let text = String::from_utf8(reply).unwrap();
                    let value = text.trim_end().rsplit("\r\n").next().unwrap();
                    got.push(value.parse::<usize>().unwrap());
                }
                got
            })
        })
        .collect();

    let mut pusher = Client::connect(addr).await;
    for n in 0..WAITERS * PER_WAITER {
        pusher.send(&format!("RPUSH q {n}")).await;
        if n % 7 == 0 {
            tokio::task::yield_now().await;
        }
    }

    let mut all = Vec::new();
    for waiter in waiters {
        all.extend(waiter.await.unwrap());
    }
    all.sort_unstable();
    assert_eq!(all, (0..WAITERS * PER_WAITER).collect::<Vec<_>>());
}
