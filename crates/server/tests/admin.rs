//! End-to-end tests for the administrative commands that involve more than
//! one connection.

use std::net::SocketAddr;
use std::time::Duration;

use rexyra_server::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};

const TIMEOUT: Duration = Duration::from_secs(5);
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

    /// Sends a command whose reply is a bulk string and returns its text.
    async fn bulk(&mut self, command: &str) -> String {
        self.send(command).await;
        let mut header = Vec::new();
        let mut byte = [0u8; 1];
        while !header.ends_with(b"\r\n") {
            timeout(TIMEOUT, self.0.read_exact(&mut byte))
                .await
                .unwrap()
                .unwrap();
            header.push(byte[0]);
        }
        let header = String::from_utf8(header).unwrap();
        let len: usize = header
            .strip_prefix('$')
            .unwrap_or_else(|| panic!("not a bulk string: {header:?}"))
            .trim()
            .parse()
            .unwrap();
        let mut body = vec![0u8; len + 2];
        timeout(TIMEOUT, self.0.read_exact(&mut body))
            .await
            .unwrap()
            .unwrap();
        body.truncate(len);
        String::from_utf8(body).unwrap()
    }

    /// This client's own id.
    async fn id(&mut self) -> u64 {
        let info = self.bulk("CLIENT INFO").await;
        field(&info, "id").parse().unwrap()
    }

    async fn expect_silence(&mut self) {
        let mut byte = [0u8; 1];
        let read = timeout(SETTLE, self.0.read(&mut byte)).await;
        assert!(read.is_err(), "unexpected data: {read:?} {byte:?}");
    }

    /// Asserts that the server closed the connection.
    async fn expect_closed(&mut self) {
        let mut byte = [0u8; 1];
        let read = timeout(TIMEOUT, self.0.read(&mut byte))
            .await
            .expect("connection still open");
        assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");
    }
}

/// The value of `name=` in a `CLIENT LIST` line, or of `name:` in `INFO`.
fn field(text: &str, name: &str) -> String {
    text.split(|c: char| c.is_whitespace())
        .find_map(|word| {
            word.strip_prefix(name)
                .and_then(|rest| rest.strip_prefix(['=', ':']))
        })
        .unwrap_or_else(|| panic!("no {name} in {text:?}"))
        .to_owned()
}

#[tokio::test]
async fn client_list_shows_every_connection() {
    let addr = start().await;
    let mut a = Client::connect(addr).await;
    let mut b = Client::connect(addr).await;
    b.check("CLIENT SETNAME worker", "+OK\r\n").await;
    b.check("SELECT 4", "+OK\r\n").await;

    let list = a.bulk("CLIENT LIST").await;
    let lines: Vec<&str> = list.lines().collect();
    assert_eq!(lines.len(), 2, "{list}");
    let (mine, theirs) = (lines[0], lines[1]);
    assert_eq!(field(mine, "addr"), a.0.local_addr().unwrap().to_string());
    assert_eq!(field(mine, "laddr"), addr.to_string());
    assert_eq!(field(mine, "cmd"), "client|list");
    assert_eq!(field(theirs, "addr"), b.0.local_addr().unwrap().to_string());
    assert_eq!(field(theirs, "name"), "worker");
    assert_eq!(field(theirs, "db"), "4");
    assert_eq!(field(theirs, "cmd"), "select");

    drop(b);
    sleep(SETTLE).await;
    assert_eq!(a.bulk("CLIENT LIST").await.lines().count(), 1);
}

#[tokio::test]
async fn info_counts_clients_and_commands() {
    let addr = start().await;
    let mut a = Client::connect(addr).await;
    let mut b = Client::connect(addr).await;
    b.send("BLPOP nothing 0").await;
    sleep(SETTLE).await;

    let info = a.bulk("INFO").await;
    assert_eq!(field(&info, "connected_clients"), "2");
    assert_eq!(field(&info, "blocked_clients"), "1");
    assert_eq!(field(&info, "total_connections_received"), "2");
    assert_eq!(field(&info, "tcp_port"), addr.port().to_string());
    assert_eq!(field(&info, "process_id"), std::process::id().to_string());
    assert!(field(&info, "used_memory").parse::<u64>().unwrap() > 0);
    // The blocked BLPOP has not completed, so it is not counted yet.
    assert_eq!(field(&info, "total_commands_processed"), "0");
}

#[tokio::test]
async fn client_kill_closes_the_other_connection() {
    let addr = start().await;
    let mut admin = Client::connect(addr).await;
    let mut victim = Client::connect(addr).await;
    let mut bystander = Client::connect(addr).await;
    let victim_id = victim.id().await;

    admin
        .check(&format!("CLIENT KILL ID {victim_id}"), ":1\r\n")
        .await;
    victim.expect_closed().await;
    bystander.check("PING", "+PONG\r\n").await;
    sleep(SETTLE).await;
    assert_eq!(admin.bulk("CLIENT LIST").await.lines().count(), 2);

    // By address, the old form.
    let address = bystander.0.local_addr().unwrap();
    admin
        .check(&format!("CLIENT KILL {address}"), "+OK\r\n")
        .await;
    bystander.expect_closed().await;
    admin
        .check(&format!("CLIENT KILL {address}"), "-ERR No such client\r\n")
        .await;
}

#[tokio::test]
async fn a_client_can_kill_itself_and_still_gets_the_reply() {
    let addr = start().await;
    let mut c = Client::connect(addr).await;
    let id = c.id().await;
    c.check(&format!("CLIENT KILL ID {id} SKIPME no\r\nPING"), ":1\r\n")
        .await;
    // The PING after it in the same pipeline is never answered.
    c.expect_closed().await;
}

#[tokio::test]
async fn killing_a_blocked_client_frees_its_place_in_the_queue() {
    let addr = start().await;
    let mut admin = Client::connect(addr).await;
    let mut first = Client::connect(addr).await;
    let mut second = Client::connect(addr).await;
    let first_id = first.id().await;
    first.send("BLPOP q 0").await;
    sleep(SETTLE).await;
    second.send("BLPOP q 0").await;
    sleep(SETTLE).await;

    admin
        .check(&format!("CLIENT KILL ID {first_id}"), ":1\r\n")
        .await;
    first.expect_closed().await;
    admin.check("RPUSH q a", ":1\r\n").await;
    second.expect("*2\r\n$1\r\nq\r\n$1\r\na\r\n").await;
}

#[tokio::test]
async fn client_unblock_ends_a_blocking_command() {
    let addr = start().await;
    let mut admin = Client::connect(addr).await;
    let mut waiter = Client::connect(addr).await;
    let id = waiter.id().await;

    admin.check(&format!("CLIENT UNBLOCK {id}"), ":0\r\n").await;

    waiter.send("BLPOP q 0\r\nPING").await;
    waiter.expect_silence().await;
    let line = admin.bulk(&format!("CLIENT LIST ID {id}")).await;
    assert_eq!(field(&line, "flags"), "b");
    admin.check(&format!("CLIENT UNBLOCK {id}"), ":1\r\n").await;
    // A null reply, then the rest of the pipeline.
    waiter.expect("*-1\r\n+PONG\r\n").await;

    waiter.send("BZPOPMIN z 0").await;
    waiter.expect_silence().await;
    admin
        .check(&format!("CLIENT UNBLOCK {id} ERROR"), ":1\r\n")
        .await;
    waiter
        .expect("-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n")
        .await;

    // It no longer takes elements, and is an ordinary client again.
    admin.check("RPUSH q a", ":1\r\n").await;
    waiter.expect_silence().await;
    waiter.check("LLEN q", ":1\r\n").await;
    let line = admin.bulk(&format!("CLIENT LIST ID {id}")).await;
    assert_eq!(field(&line, "flags"), "N");
}

#[tokio::test]
async fn config_set_maxclients_takes_effect() {
    let addr = start().await;
    let mut a = Client::connect(addr).await;
    a.check("CONFIG SET maxclients 2", "+OK\r\n").await;
    let mut b = Client::connect(addr).await;
    b.check("PING", "+PONG\r\n").await;

    let mut refused = Client::connect(addr).await;
    refused
        .expect("-ERR max number of clients reached\r\n")
        .await;

    a.check("CONFIG SET maxclients 3", "+OK\r\n").await;
    let mut c = Client::connect(addr).await;
    c.check("PING", "+PONG\r\n").await;
}

#[tokio::test]
async fn slow_log_records_commands_over_the_threshold() {
    let addr = start().await;
    let mut c = Client::connect(addr).await;
    c.check("SLOWLOG LEN", ":0\r\n").await;
    c.check("SET k v", "+OK\r\n").await;
    // Nothing is slow enough for the default threshold of 10 ms.
    c.check("SLOWLOG LEN", ":0\r\n").await;
    c.check("CONFIG SET slowlog-log-slower-than 0", "+OK\r\n")
        .await;
    c.check("SET k v", "+OK\r\n").await;
    c.check("SLOWLOG LEN", ":2\r\n").await;
    c.check("SLOWLOG RESET", "+OK\r\n").await;
    c.check("SLOWLOG LEN", ":1\r\n").await;
}
