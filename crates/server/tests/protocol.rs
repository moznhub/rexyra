//! End-to-end tests: a real server on a loopback port, driven with raw bytes
//! exactly as a client would send them.

use std::net::SocketAddr;
use std::time::Duration;

use rexyra_server::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);

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

    async fn send(&mut self, bytes: &[u8]) {
        self.0.write_all(bytes).await.unwrap();
    }

    /// Reads exactly as many bytes as `expected` and compares.
    async fn expect(&mut self, expected: &str) {
        let mut got = vec![0u8; expected.len()];
        timeout(TIMEOUT, self.0.read_exact(&mut got))
            .await
            .expect("timed out waiting for reply")
            .expect("connection closed early");
        assert_eq!(String::from_utf8_lossy(&got), expected);
    }

    async fn roundtrip(&mut self, request: &[u8], expected: &str) {
        self.send(request).await;
        self.expect(expected).await;
    }

    /// Asserts the server closes the connection without sending more data.
    async fn expect_closed(&mut self) {
        let mut rest = Vec::new();
        timeout(TIMEOUT, self.0.read_to_end(&mut rest))
            .await
            .expect("timed out waiting for close")
            .unwrap();
        assert!(
            rest.is_empty(),
            "unexpected trailing data: {:?}",
            String::from_utf8_lossy(&rest)
        );
    }
}

#[tokio::test]
async fn ping_and_echo() {
    let mut c = Client::connect(start().await).await;
    c.roundtrip(b"*1\r\n$4\r\nPING\r\n", "+PONG\r\n").await;
    c.roundtrip(b"*2\r\n$4\r\nPING\r\n$2\r\nhi\r\n", "$2\r\nhi\r\n")
        .await;
    c.roundtrip(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n", "$5\r\nhello\r\n")
        .await;
}

#[tokio::test]
async fn inline_commands() {
    let mut c = Client::connect(start().await).await;
    c.roundtrip(b"PING\r\n", "+PONG\r\n").await;
    c.roundtrip(b"echo \"a b\"\n", "$3\r\na b\r\n").await;
}

#[tokio::test]
async fn pipelining_preserves_order() {
    let mut c = Client::connect(start().await).await;
    let mut request = Vec::new();
    let mut expected = String::new();
    for i in 0..500 {
        let payload = format!("msg-{i}");
        request.extend(format!("*2\r\n$4\r\nECHO\r\n${}\r\n{payload}\r\n", payload.len()).bytes());
        expected.push_str(&format!("${}\r\n{payload}\r\n", payload.len()));
    }
    c.roundtrip(&request, &expected).await;
}

#[tokio::test]
async fn request_split_across_packets() {
    let mut c = Client::connect(start().await).await;
    for chunk in [&b"*2\r\n$4\r\nEC"[..], b"HO\r\n$5\r\nhe", b"llo\r\n"] {
        c.send(chunk).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    c.expect("$5\r\nhello\r\n").await;
}

#[tokio::test]
async fn hello_switches_to_resp3() {
    let mut c = Client::connect(start().await).await;
    // RESP2 null first, to prove the switch really changes the encoding.
    c.roundtrip(b"CLIENT GETNAME\r\n", "$-1\r\n").await;
    c.roundtrip(
        b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n",
        "%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.4.0\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n",
    )
    .await;
    // Skip the rest of the HELLO map by issuing a command with a known reply.
    c.send(b"CLIENT GETNAME\r\n").await;
    let mut seen = Vec::new();
    let mut byte = [0u8; 1];
    while !seen.ends_with(b"_\r\n") {
        timeout(TIMEOUT, c.0.read_exact(&mut byte))
            .await
            .expect("RESP3 null never arrived")
            .unwrap();
        seen.push(byte[0]);
        assert!(seen.len() < 512, "{:?}", String::from_utf8_lossy(&seen));
    }
}

#[tokio::test]
async fn hello_in_resp2_returns_flat_array() {
    let mut c = Client::connect(start().await).await;
    c.roundtrip(b"HELLO\r\n", "*14\r\n$6\r\nserver\r\n").await;
}

#[tokio::test]
async fn errors_keep_the_connection_open() {
    let mut c = Client::connect(start().await).await;
    c.roundtrip(
        b"FOO bar\r\n",
        "-ERR unknown command 'FOO', with args beginning with: 'bar' \r\n",
    )
    .await;
    c.roundtrip(
        b"ECHO\r\n",
        "-ERR wrong number of arguments for 'echo' command\r\n",
    )
    .await;
    c.roundtrip(b"SELECT 99\r\n", "-ERR DB index is out of range\r\n")
        .await;
    c.roundtrip(b"SELECT 1\r\n", "+OK\r\n").await;
    c.roundtrip(b"PING\r\n", "+PONG\r\n").await;
}

#[tokio::test]
async fn protocol_error_answers_earlier_commands_then_closes() {
    let mut c = Client::connect(start().await).await;
    c.send(b"*1\r\n$4\r\nPING\r\n*1\r\n:oops\r\n").await;
    c.expect("+PONG\r\n-ERR Protocol error: expected '$', got ':'\r\n")
        .await;
    c.expect_closed().await;
}

#[tokio::test]
async fn quit_closes_and_skips_later_commands() {
    let mut c = Client::connect(start().await).await;
    c.send(b"PING\r\nQUIT\r\nPING\r\n").await;
    c.expect("+PONG\r\n+OK\r\n").await;
    c.expect_closed().await;
}

#[tokio::test]
async fn clients_have_independent_state_and_ids() {
    let addr = start().await;
    let mut a = Client::connect(addr).await;
    let mut b = Client::connect(addr).await;
    a.roundtrip(b"CLIENT SETNAME alpha\r\n", "+OK\r\n").await;
    b.roundtrip(b"CLIENT GETNAME\r\n", "$-1\r\n").await;
    a.roundtrip(b"CLIENT GETNAME\r\n", "$5\r\nalpha\r\n").await;
    a.roundtrip(b"CLIENT ID\r\n", ":1\r\n").await;
    b.roundtrip(b"CLIENT ID\r\n", ":2\r\n").await;
}

#[tokio::test]
async fn command_count() {
    let mut c = Client::connect(start().await).await;
    let expected = format!(":{}\r\n", rexyra_commands::COMMANDS.len());
    c.roundtrip(b"COMMAND COUNT\r\n", &expected).await;
}

#[tokio::test]
async fn maxclients_is_enforced_and_slots_are_released() {
    let addr = start_with(Config {
        maxclients: 1,
        ..Config::default()
    })
    .await;

    let mut first = Client::connect(addr).await;
    first.roundtrip(b"PING\r\n", "+PONG\r\n").await;

    let mut second = Client::connect(addr).await;
    second
        .expect("-ERR max number of clients reached\r\n")
        .await;
    second.expect_closed().await;

    drop(first);
    // The slot is released asynchronously once the server notices the close.
    for attempt in 0.. {
        let mut retry = Client::connect(addr).await;
        retry.send(b"PING\r\n").await;
        let mut reply = [0u8; 7];
        timeout(TIMEOUT, retry.0.read_exact(&mut reply))
            .await
            .unwrap()
            .unwrap();
        if &reply == b"+PONG\r\n" {
            break;
        }
        assert!(attempt < 50, "slot was never released");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn large_binary_payload() {
    let mut c = Client::connect(start().await).await;
    let payload: Vec<u8> = (0..1_000_000u32).map(|i| (i % 251) as u8).collect();
    let mut request = format!("*2\r\n$4\r\nECHO\r\n${}\r\n", payload.len()).into_bytes();
    request.extend(&payload);
    request.extend(b"\r\n");
    c.send(&request).await;

    let header = format!("${}\r\n", payload.len());
    c.expect(&header).await;
    let mut got = vec![0u8; payload.len() + 2];
    timeout(TIMEOUT, c.0.read_exact(&mut got))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&got[..payload.len()], &payload[..]);
    assert_eq!(&got[payload.len()..], b"\r\n");
}
