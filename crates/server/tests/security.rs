//! End-to-end tests of authentication, users and protected mode.

use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::time::Duration;

use rexyra_server::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);
const NOAUTH: &str = "-NOAUTH Authentication required.";
const WRONGPASS: &str = "-WRONGPASS invalid username-password pair or user is disabled.";

async fn start_on(bind: &str, config: Config) -> SocketAddr {
    let listener = TcpListener::bind((bind, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(rexyra_server::serve(
        listener,
        config,
        std::future::pending(),
    ));
    addr
}

async fn start(config: Config) -> SocketAddr {
    start_on("127.0.0.1", config).await
}

struct Client(TcpStream);

impl Client {
    async fn connect(addr: SocketAddr) -> Client {
        Client(TcpStream::connect(addr).await.unwrap())
    }

    /// Sends one command and returns the first line of the reply.
    async fn ask(&mut self, command: &str) -> String {
        self.0
            .write_all(format!("{command}\r\n").as_bytes())
            .await
            .unwrap();
        self.line().await.expect("connection closed")
    }

    async fn line(&mut self) -> Option<String> {
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        while !line.ends_with(b"\r\n") {
            match timeout(TIMEOUT, self.0.read(&mut byte)).await {
                Ok(Ok(1)) => line.push(byte[0]),
                Ok(_) => return None,
                Err(_) => panic!("timed out waiting for a reply"),
            }
        }
        Some(String::from_utf8(line).unwrap().trim_end().to_owned())
    }

    /// Sends a command whose reply is a bulk string and returns its text.
    async fn bulk(&mut self, command: &str) -> String {
        let header = self.ask(command).await;
        assert!(header.starts_with('$'), "{header}");
        self.line().await.unwrap()
    }
}

#[tokio::test]
async fn a_password_is_required_once_configured() {
    let addr = start(Config {
        requirepass: "s3cret".to_owned(),
        ..Config::default()
    })
    .await;
    let mut c = Client::connect(addr).await;
    for command in ["PING", "GET k", "SET k v", "INFO", "CONFIG GET requirepass"] {
        assert_eq!(c.ask(command).await, NOAUTH, "{command}");
    }
    assert_eq!(c.ask("AUTH wrong").await, WRONGPASS);
    assert_eq!(c.ask("AUTH default wrong").await, WRONGPASS);
    assert_eq!(c.ask("PING").await, NOAUTH);
    assert_eq!(c.ask("AUTH s3cret").await, "+OK");
    assert_eq!(c.ask("SET k v").await, "+OK");
    assert_eq!(c.bulk("ACL WHOAMI").await, "default");

    // Each connection authenticates for itself.
    let mut other = Client::connect(addr).await;
    assert_eq!(other.ask("GET k").await, NOAUTH);
    assert!(other
        .ask("HELLO 3 AUTH default s3cret")
        .await
        .starts_with('%'));

    // Changing the password leaves those logged in alone and applies to
    // whoever comes next.
    assert_eq!(c.ask("CONFIG SET requirepass other").await, "+OK");
    assert_eq!(c.ask("PING").await, "+PONG");
    let mut late = Client::connect(addr).await;
    assert_eq!(late.ask("AUTH s3cret").await, WRONGPASS);
    assert_eq!(late.ask("AUTH other").await, "+OK");
    // A pipeline is refused command by command until it authenticates.
    let mut piped = Client::connect(addr).await;
    piped
        .0
        .write_all(b"GET k\r\nAUTH other\r\nGET k\r\n")
        .await
        .unwrap();
    assert_eq!(piped.line().await.unwrap(), NOAUTH);
    assert_eq!(piped.line().await.unwrap(), "+OK");
    assert_eq!(piped.line().await.unwrap(), "$1");
}

#[tokio::test]
async fn users_from_the_configuration_are_held_to_their_permissions() {
    let mut config = Config::default();
    config
        .apply_file(
            "user worker on >pw ~jobs:* +@list +ping\n\
             user reader on nopass %R~* +@read -@dangerous\n\
             user gone off >pw allkeys +@all\n",
        )
        .unwrap();
    let addr = start(config).await;

    let mut admin = Client::connect(addr).await;
    assert_eq!(admin.ask("SET plain v").await, "+OK");
    assert_eq!(admin.ask("ACL USERS").await, "*4");
    let mut names = Vec::new();
    for _ in 0..4 {
        admin.line().await.unwrap();
        names.push(admin.line().await.unwrap());
    }
    assert_eq!(names, ["default", "gone", "reader", "worker"]);

    let mut worker = Client::connect(addr).await;
    assert_eq!(worker.ask("AUTH gone pw").await, WRONGPASS);
    assert_eq!(worker.ask("AUTH worker pw").await, "+OK");
    assert_eq!(worker.ask("RPUSH jobs:1 a b").await, ":2");
    assert_eq!(
        worker.ask("RPUSH other a").await,
        "-NOPERM No permissions to access a key"
    );
    assert_eq!(
        worker.ask("GET plain").await,
        "-NOPERM User worker has no permissions to run the 'get' command"
    );
    assert_eq!(
        worker.ask("FLUSHALL").await,
        "-NOPERM User worker has no permissions to run the 'flushall' command"
    );
    // A blocking command is checked like any other, and works when allowed.
    assert_eq!(
        worker.ask("BLPOP other 1").await,
        "-NOPERM No permissions to access a key"
    );
    assert_eq!(worker.ask("BLPOP jobs:1 1").await, "*2");
    for expected in ["$6", "jobs:1", "$1", "a"] {
        assert_eq!(worker.line().await.unwrap(), expected);
    }

    let mut reader = Client::connect(addr).await;
    assert_eq!(reader.ask("AUTH reader anything").await, "+OK");
    assert_eq!(reader.bulk("GET plain").await, "v");
    assert_eq!(
        reader.ask("SET plain w").await,
        "-NOPERM User reader has no permissions to run the 'set' command"
    );
    assert_eq!(
        reader.ask("KEYS *").await,
        "-NOPERM User reader has no permissions to run the 'keys' command"
    );
    let list = admin.ask("CLIENT LIST").await;
    assert!(list.starts_with('$'), "{list}");
    let body = admin.line().await.unwrap();
    let mut users: Vec<&str> = body
        .lines()
        .filter_map(|line| line.split(' ').find(|field| field.starts_with("user=")))
        .collect();
    users.sort_unstable();
    assert_eq!(users, ["user=default", "user=reader", "user=worker"]);

    // Deleting a user closes its connections.
    assert_eq!(admin.ask("ACL DELUSER reader").await, ":1");
    assert_eq!(reader.line().await, None, "the connection stayed open");
    assert_eq!(worker.ask("PING").await, "+PONG");
}

/// An address of this machine that is not the loopback one, if it has any.
fn lan_address() -> Option<IpAddr> {
    // Connecting a UDP socket sends nothing; it only picks the route.
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("192.0.2.1:9").ok()?;
    let ip = socket.local_addr().ok()?.ip();
    (!ip.is_loopback() && !ip.is_unspecified()).then_some(ip)
}

#[tokio::test]
async fn protected_mode_turns_other_machines_away_until_a_password_is_set() {
    let Some(lan) = lan_address() else {
        eprintln!("no network address besides loopback; skipping");
        return;
    };
    let addr = start_on("0.0.0.0", Config::default()).await;
    let outside = SocketAddr::new(lan, addr.port());
    let inside = SocketAddr::new("127.0.0.1".parse().unwrap(), addr.port());

    // Arriving over the network address, the connection is refused ...
    let mut stranger = Client::connect(outside).await;
    let denial = stranger.line().await.unwrap();
    assert!(
        denial.starts_with("-DENIED The server is running in protected mode"),
        "{denial}"
    );
    assert_eq!(stranger.line().await, None);
    // ... from this machine it is not.
    let mut local = Client::connect(inside).await;
    assert_eq!(local.ask("PING").await, "+PONG");

    // With a password set, anyone who knows it may connect.
    assert_eq!(local.ask("CONFIG SET requirepass pw").await, "+OK");
    let mut remote = Client::connect(outside).await;
    assert_eq!(remote.ask("PING").await, NOAUTH);
    assert_eq!(remote.ask("AUTH pw").await, "+OK");
    assert_eq!(remote.ask("PING").await, "+PONG");

    // Without one, only if protected mode is turned off.
    assert_eq!(local.ask("CONFIG SET requirepass \"\"").await, "+OK");
    let mut stranger = Client::connect(outside).await;
    assert!(stranger.line().await.unwrap().starts_with("-DENIED"));
    assert_eq!(local.ask("CONFIG SET protected-mode no").await, "+OK");
    let mut trusted = Client::connect(outside).await;
    assert_eq!(trusted.ask("PING").await, "+PONG");
}
