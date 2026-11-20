//! End-to-end tests for data commands: real sockets, real clock.

use std::net::SocketAddr;
use std::time::Duration;

use rexyra_server::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);

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

    /// Sends an inline command and asserts the exact reply bytes.
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
async fn set_get_del_over_the_wire() {
    let mut c = Client::connect(start().await).await;
    c.check("GET greeting", "$-1\r\n").await;
    c.check("SET greeting hello", "+OK\r\n").await;
    c.check("GET greeting", "$5\r\nhello\r\n").await;
    c.check("APPEND greeting !", ":6\r\n").await;
    c.check("INCR counter", ":1\r\n").await;
    c.check(
        "MGET greeting counter nope",
        "*3\r\n$6\r\nhello!\r\n$1\r\n1\r\n$-1\r\n",
    )
    .await;
    c.check("DBSIZE", ":2\r\n").await;
    c.check("DEL greeting counter", ":2\r\n").await;
    c.check("DBSIZE", ":0\r\n").await;
}

#[tokio::test]
async fn data_is_shared_between_connections_and_scoped_by_database() {
    let addr = start().await;
    let mut a = Client::connect(addr).await;
    let mut b = Client::connect(addr).await;
    a.check("SET shared 1", "+OK\r\n").await;
    b.check("GET shared", "$1\r\n1\r\n").await;
    b.check("SELECT 3", "+OK\r\n").await;
    b.check("GET shared", "$-1\r\n").await;
    b.check("SET shared three", "+OK\r\n").await;
    a.check("GET shared", "$1\r\n1\r\n").await;
}

#[tokio::test]
async fn concurrent_increments_are_atomic() {
    let addr = start().await;
    let workers: Vec<_> = (0..8)
        .map(|_| {
            tokio::spawn(async move {
                let mut stream = TcpStream::connect(addr).await.unwrap();
                // 250 pipelined INCRs per connection.
                stream
                    .write_all("INCR hits\r\n".repeat(250).as_bytes())
                    .await
                    .unwrap();
                // Each reply is ":<n>\r\n"; count the newlines.
                let mut replies = 0;
                let mut buf = [0u8; 4096];
                while replies < 250 {
                    let n = timeout(TIMEOUT, stream.read(&mut buf))
                        .await
                        .unwrap()
                        .unwrap();
                    assert_ne!(n, 0);
                    replies += buf[..n].iter().filter(|&&b| b == b'\n').count();
                }
            })
        })
        .collect();
    for worker in workers {
        worker.await.unwrap();
    }
    let mut c = Client::connect(addr).await;
    c.check("GET hits", "$4\r\n2000\r\n").await;
}

#[tokio::test]
async fn keys_expire_on_the_real_clock() {
    let mut c = Client::connect(start().await).await;
    c.check("SET temp v PX 80", "+OK\r\n").await;
    c.check("GET temp", "$1\r\nv\r\n").await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    c.check("GET temp", "$-1\r\n").await;
    c.check("TTL temp", ":-2\r\n").await;
}

#[tokio::test]
async fn expired_keys_are_removed_without_being_read() {
    let mut c = Client::connect(start().await).await;
    for i in 0..50 {
        c.check(&format!("SET temp:{i} v PX 50"), "+OK\r\n").await;
    }
    c.check("SET keep v", "+OK\r\n").await;
    c.check("DBSIZE", ":51\r\n").await;

    // DBSIZE does not trigger lazy expiry, so the count can only drop if the
    // background cycle is doing its job.
    for attempt in 0.. {
        tokio::time::sleep(Duration::from_millis(50)).await;
        c.0.write_all(b"DBSIZE\r\n").await.unwrap();
        let mut reply = Vec::new();
        let mut byte = [0u8; 1];
        while !reply.ends_with(b"\r\n") {
            timeout(TIMEOUT, c.0.read_exact(&mut byte))
                .await
                .unwrap()
                .unwrap();
            reply.push(byte[0]);
        }
        if reply == b":1\r\n" {
            break;
        }
        assert!(
            attempt < 40,
            "active expiry never ran; DBSIZE is {:?}",
            String::from_utf8_lossy(&reply)
        );
    }
}

#[tokio::test]
async fn watch_detects_a_write_from_another_connection() {
    let addr = start().await;
    let mut a = Client::connect(addr).await;
    let mut b = Client::connect(addr).await;

    a.check("SET balance 10", "+OK\r\n").await;
    a.check("WATCH balance", "+OK\r\n").await;
    b.check("DECRBY balance 3", ":7\r\n").await;
    a.check("MULTI", "+OK\r\n").await;
    a.check("SET balance 0", "+QUEUED\r\n").await;
    a.check("EXEC", "*-1\r\n").await;
    a.check("GET balance", "$1\r\n7\r\n").await;

    // Undisturbed, the same sequence goes through, as one pipeline too.
    a.check(
        "WATCH balance\r\nMULTI\r\nDECRBY balance 2\r\nGET balance\r\nEXEC",
        "+OK\r\n+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n:5\r\n$1\r\n5\r\n",
    )
    .await;
}

#[tokio::test]
async fn a_transaction_is_not_interleaved_with_other_clients() {
    let addr = start().await;
    let workers: Vec<_> = (0..8)
        .map(|_| {
            tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                for _ in 0..50 {
                    // Read-modify-write that is only correct if atomic.
                    c.check(
                        "MULTI\r\nINCR n\r\nINCR n\r\nDECR n\r\nEXEC",
                        "+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n",
                    )
                    .await;
                    let mut numbers = Vec::new();
                    while numbers.len() < 3 {
                        let mut line = Vec::new();
                        let mut byte = [0u8; 1];
                        while !line.ends_with(b"\r\n") {
                            timeout(TIMEOUT, c.0.read_exact(&mut byte))
                                .await
                                .unwrap()
                                .unwrap();
                            line.push(byte[0]);
                        }
                        let text = String::from_utf8(line).unwrap();
                        numbers.push(text.trim().trim_start_matches(':').parse::<i64>().unwrap());
                    }
                    assert_eq!(numbers[1], numbers[0] + 1);
                    assert_eq!(numbers[2], numbers[0]);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.await.unwrap();
    }
    let mut c = Client::connect(addr).await;
    c.check("GET n", "$3\r\n400\r\n").await;
}

#[tokio::test]
async fn a_disconnected_client_leaves_no_watches_or_transaction_behind() {
    let addr = start().await;
    let mut gone = Client::connect(addr).await;
    gone.check("WATCH k", "+OK\r\n").await;
    gone.check("MULTI", "+OK\r\n").await;
    gone.check("SET k never", "+QUEUED\r\n").await;
    drop(gone);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut c = Client::connect(addr).await;
    c.check("EXISTS k", ":0\r\n").await;
    c.check("SET k 1", "+OK\r\n").await;
    c.check("GET k", "$1\r\n1\r\n").await;
}

#[tokio::test]
async fn pipelines_from_many_clients_are_all_answered() {
    const CLIENTS: usize = 50;
    const ROUNDS: usize = 25;
    const DEPTH: usize = 16;
    let addr = start().await;
    let workers: Vec<_> = (0..CLIENTS)
        .map(|client| {
            tokio::spawn(async move {
                let mut stream = TcpStream::connect(addr).await.unwrap();
                let command = format!("*3\r\n$3\r\nSET\r\n$7\r\nkey:{client:03}\r\n$3\r\nxxx\r\n");
                let pipeline = command.repeat(DEPTH);
                // Split each pipeline at an awkward place, as a network may.
                let (head, tail) = pipeline.as_bytes().split_at(pipeline.len() / 3 + client);
                let mut reply = vec![0u8; "+OK\r\n".len() * DEPTH];
                for round in 0..ROUNDS {
                    stream.write_all(head).await.unwrap();
                    if round % 2 == 0 {
                        tokio::task::yield_now().await;
                    }
                    stream.write_all(tail).await.unwrap();
                    timeout(TIMEOUT, stream.read_exact(&mut reply))
                        .await
                        .unwrap_or_else(|_| panic!("client {client} starved in round {round}"))
                        .unwrap();
                    assert_eq!(reply, "+OK\r\n".repeat(DEPTH).as_bytes());
                }
            })
        })
        .collect();
    for worker in workers {
        worker.await.unwrap();
    }
    let mut c = Client::connect(addr).await;
    c.check("DBSIZE", ":50\r\n").await;
}
