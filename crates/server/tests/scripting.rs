//! End-to-end tests for Lua scripting over real connections.

use std::net::SocketAddr;
use std::time::Duration;

use rexyra_server::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);

/// Check and decrement in one step; never lets the balance go below zero.
const TRY_TAKE: &str = "\
local current = redis.call('GET', KEYS[1])
if not current then
    return false
end
if tonumber(current) < tonumber(ARGV[1]) then
    return -1
end
return redis.call('DECRBY', KEYS[1], ARGV[1])";

async fn start() -> SocketAddr {
    start_with(Config::default()).await
}

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

/// A server that calls a script busy after 100 ms.
async fn start_impatient() -> SocketAddr {
    start_with(Config {
        busy_reply_threshold: 100,
        ..Config::default()
    })
    .await
}

const BUSY: &str =
    "-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.";

struct Client(TcpStream);

impl Client {
    async fn connect(addr: SocketAddr) -> Client {
        Client(TcpStream::connect(addr).await.unwrap())
    }

    /// Sends a command as a RESP array, so arguments may contain anything.
    async fn send(&mut self, args: &[&str]) {
        let mut request = format!("*{}\r\n", args.len());
        for arg in args {
            request.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
        }
        self.0.write_all(request.as_bytes()).await.unwrap();
    }

    /// Reads one line of a reply, without the line ending.
    async fn line(&mut self) -> String {
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        while !line.ends_with(b"\r\n") {
            timeout(TIMEOUT, self.0.read_exact(&mut byte))
                .await
                .expect("timed out waiting for reply")
                .expect("connection closed early");
            line.push(byte[0]);
        }
        line.truncate(line.len() - 2);
        String::from_utf8(line).unwrap()
    }

    /// Sends a command and returns the first line of its reply.
    async fn call(&mut self, args: &[&str]) -> String {
        self.send(args).await;
        self.line().await
    }

    /// Sends a command whose reply is a bulk string and returns its text.
    /// With no arguments, reads the reply to a command already sent.
    async fn bulk(&mut self, args: &[&str]) -> String {
        let header = if args.is_empty() {
            self.line().await
        } else {
            self.call(args).await
        };
        assert!(header.starts_with('$'), "not a bulk string: {header}");
        self.line().await
    }
}

#[tokio::test]
async fn a_loaded_script_is_available_to_every_connection() {
    let addr = start().await;
    let mut a = Client::connect(addr).await;
    let mut b = Client::connect(addr).await;

    let sha = a.bulk(&["SCRIPT", "LOAD", TRY_TAKE]).await;
    assert_eq!(sha.len(), 40);
    assert_eq!(a.call(&["SET", "stock", "5"]).await, "+OK");

    assert_eq!(b.call(&["EVALSHA", &sha, "1", "stock", "2"]).await, ":3");
    assert_eq!(a.call(&["EVALSHA", &sha, "1", "stock", "9"]).await, ":-1");
    // Missing key: a null reply.
    assert_eq!(b.call(&["EVALSHA", &sha, "1", "nothing", "1"]).await, "$-1");
    assert_eq!(
        b.call(&["EVALSHA", &"0".repeat(40), "0"]).await,
        "-NOSCRIPT No matching script. Please use EVAL."
    );
}

#[tokio::test]
async fn concurrent_takes_never_overdraw() {
    const CLIENTS: usize = 16;
    const ATTEMPTS: usize = 50;
    const STOCK: usize = 300;
    let addr = start().await;
    let mut setup = Client::connect(addr).await;
    let sha = setup.bulk(&["SCRIPT", "LOAD", TRY_TAKE]).await;
    assert_eq!(
        setup.call(&["SET", "stock", &STOCK.to_string()]).await,
        "+OK"
    );

    let workers: Vec<_> = (0..CLIENTS)
        .map(|_| {
            let sha = sha.clone();
            tokio::spawn(async move {
                let mut c = Client::connect(addr).await;
                let mut taken = 0;
                for _ in 0..ATTEMPTS {
                    let reply = c.call(&["EVALSHA", &sha, "1", "stock", "1"]).await;
                    let left: i64 = reply.trim_start_matches(':').parse().unwrap();
                    // -1 means refused; anything else is the new balance.
                    assert!(left >= -1, "{reply}");
                    if left >= 0 {
                        taken += 1;
                    }
                }
                taken
            })
        })
        .collect();

    let mut taken = 0;
    for worker in workers {
        taken += worker.await.unwrap();
    }
    // 800 attempts on a stock of 300: exactly 300 succeed.
    assert_eq!(taken, STOCK);
    assert_eq!(setup.bulk(&["GET", "stock"]).await, "0");
}

#[tokio::test]
async fn a_script_that_pushes_wakes_a_blocked_client() {
    let addr = start().await;
    let mut waiter = Client::connect(addr).await;
    let mut producer = Client::connect(addr).await;

    waiter.send(&["BLPOP", "jobs", "0"]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The script sees its own push; the waiter is served once it finishes.
    let reply = producer
        .call(&[
            "EVAL",
            "redis.call('RPUSH', KEYS[1], 'a', 'b') return redis.call('LLEN', KEYS[1])",
            "1",
            "jobs",
        ])
        .await;
    assert_eq!(reply, ":2");

    assert_eq!(waiter.line().await, "*2");
    assert_eq!(waiter.line().await, "$4");
    assert_eq!(waiter.line().await, "jobs");
    assert_eq!(waiter.line().await, "$1");
    assert_eq!(waiter.line().await, "a");
    assert_eq!(producer.call(&["LLEN", "jobs"]).await, ":1");
}

#[tokio::test]
async fn script_errors_reach_the_client_and_leave_the_server_working() {
    let addr = start().await;
    let mut c = Client::connect(addr).await;
    let reply = c.call(&["EVAL", "return redis.call('NOSUCH')", "0"]).await;
    assert!(
        reply.starts_with("-ERR Unknown Redis command called from script script: "),
        "{reply}"
    );
    assert!(reply.ends_with(", on @user_script:1."), "{reply}");
    assert_eq!(c.call(&["EVAL", "return 1 + 1", "0"]).await, ":2");
    assert_eq!(c.call(&["PING"]).await, "+PONG");
}

#[tokio::test]
async fn a_runaway_script_is_reported_busy_and_can_be_killed() {
    let addr = start_impatient().await;
    let mut runaway = Client::connect(addr).await;
    let mut other = Client::connect(addr).await;

    runaway.send(&["EVAL", "while true do end", "0"]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Other clients are not left hanging: they are told the server is busy.
    assert_eq!(other.call(&["PING"]).await, BUSY);
    assert_eq!(other.call(&["SET", "k", "v"]).await, BUSY);
    // A client that connects meanwhile is told the same.
    let mut late = Client::connect(addr).await;
    assert_eq!(late.call(&["PING"]).await, BUSY);

    assert_eq!(other.call(&["SCRIPT", "KILL"]).await, "+OK");
    let reply = runaway.line().await;
    assert!(
        reply.starts_with("-ERR Script killed by user with SCRIPT KILL... script: "),
        "{reply}"
    );

    // Everyone is served normally again.
    assert_eq!(other.call(&["PING"]).await, "+PONG");
    assert_eq!(late.call(&["PING"]).await, "+PONG");
    assert_eq!(runaway.call(&["EVAL", "return 1", "0"]).await, ":1");
    assert_eq!(
        other.call(&["SCRIPT", "KILL"]).await,
        "-NOTBUSY No scripts in execution right now."
    );
    assert_eq!(other.call(&["EXISTS", "k"]).await, ":0");
}

#[tokio::test]
async fn a_script_that_wrote_refuses_to_be_killed() {
    let addr = start_impatient().await;
    let mut writer = Client::connect(addr).await;
    let mut other = Client::connect(addr).await;

    // Writes, then spins for about a second before finishing on its own.
    writer
        .send(&[
            "EVAL",
            "redis.call('SET', 'k', 'v') \
             local stop = os.clock() + 1 while os.clock() < stop do end return 'done'",
            "0",
        ])
        .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let reply = other.call(&["SCRIPT", "KILL"]).await;
    assert!(
        reply.starts_with("-UNKILLABLE Sorry the script already executed write commands"),
        "{reply}"
    );
    assert_eq!(other.call(&["GET", "k"]).await, BUSY);

    assert_eq!(writer.bulk(&[]).await, "done");
    assert_eq!(other.bulk(&["GET", "k"]).await, "v");
}
