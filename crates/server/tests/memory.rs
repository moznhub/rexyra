//! End-to-end test of the memory limit, measured by the real allocator.
//!
//! The allocator's count covers the whole process, so everything here runs
//! in one test against one server: tests running side by side would see
//! each other's memory.

use std::time::Duration;

use rexyra_server::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(20);

struct Client(TcpStream);

impl Client {
    /// Sends one command and returns its reply line, or for a bulk string
    /// its contents.
    async fn ask(&mut self, command: &str) -> String {
        self.0
            .write_all(format!("{command}\r\n").as_bytes())
            .await
            .unwrap();
        self.reply().await
    }

    async fn reply(&mut self) -> String {
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        while !line.ends_with(b"\r\n") {
            timeout(TIMEOUT, self.0.read_exact(&mut byte))
                .await
                .expect("timed out waiting for reply")
                .expect("connection closed early");
            line.push(byte[0]);
        }
        let line = String::from_utf8(line).unwrap().trim_end().to_owned();
        let Some(len) = line.strip_prefix('$') else {
            return line;
        };
        let Ok(len) = len.parse::<usize>() else {
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

    async fn info(&mut self, field: &str) -> u64 {
        let info = self.ask("INFO memory stats").await;
        info.lines()
            .find_map(|line| line.strip_prefix(&format!("{field}:")))
            .unwrap_or_else(|| panic!("no {field} in {info}"))
            .parse()
            .unwrap()
    }

    /// Writes `count` keys of a kilobyte each, a hundred per round trip,
    /// and returns how many of the writes were refused.
    async fn fill(&mut self, prefix: &str, count: usize) -> usize {
        let value = "v".repeat(1000);
        let mut refused = 0;
        for batch in 0..count / 100 {
            let mut request = String::new();
            for n in 0..100 {
                request.push_str(&format!("SET {prefix}:{} {value}\r\n", batch * 100 + n));
            }
            self.0.write_all(request.as_bytes()).await.unwrap();
            for _ in 0..100 {
                let reply = self.reply().await;
                if reply.starts_with("-OOM") {
                    refused += 1;
                } else {
                    assert_eq!(reply, "+OK");
                }
            }
        }
        refused
    }
}

#[tokio::test]
async fn the_memory_limit_is_enforced_against_real_memory_use() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let dir = std::env::temp_dir().join(format!("rexyra-server-memory-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let config = Config {
        dir: dir.to_str().unwrap().to_owned(),
        ..Config::default()
    };
    tokio::spawn(rexyra_server::serve(
        listener,
        config,
        std::future::pending(),
    ));
    let mut c = Client(TcpStream::connect(addr).await.unwrap());
    const MB: u64 = 1024 * 1024;

    // Memory in use is measured, and grows with the data.
    let empty = c.info("used_memory").await;
    assert!(empty > 0);
    assert_eq!(c.fill("first", 2000).await, 0);
    let with_data = c.info("used_memory").await;
    assert!(
        with_data > empty + 2 * MB && with_data < empty + 4 * MB,
        "2000 keys of 1 KB took {} bytes",
        with_data - empty
    );

    // With no eviction, writes are refused at the limit; reads are not.
    let limit = with_data + 2 * MB;
    assert_eq!(c.ask(&format!("CONFIG SET maxmemory {limit}")).await, "+OK");
    let refused = c.fill("second", 5000).await;
    assert!((2500..4500).contains(&refused), "{refused} refused");
    // (Use hovers around the limit as request buffers come and go, so the
    // limit is now put clearly below it.)
    let limit = with_data + MB;
    assert_eq!(c.ask(&format!("CONFIG SET maxmemory {limit}")).await, "+OK");
    assert_eq!(
        c.ask("SET one more").await,
        "-OOM command not allowed when used memory > 'maxmemory'."
    );
    assert_eq!(c.ask("GET first:0").await.len(), 1000);
    assert_eq!(c.ask("EXPIRE first:0 1000").await, ":1");
    assert_eq!(c.info("evicted_keys").await, 0);
    // A script may not write either.
    let reply = c
        .ask("EVAL \"return redis.call('SET', KEYS[1], 'v')\" 1 scripted")
        .await;
    assert!(reply.starts_with("-OOM command not allowed"), "{reply}");
    // Deleting makes room.
    assert_eq!(c.fill("second", 5000).await, 5000);
    let mut request = String::new();
    for n in 0..2000 {
        request.push_str(&format!(
            "DEL first:{n}
"
        ));
    }
    c.0.write_all(request.as_bytes()).await.unwrap();
    for _ in 0..2000 {
        assert_eq!(c.reply().await, ":1");
    }
    assert_eq!(c.ask("SET one more").await, "+OK");

    // With eviction, writing can go on for ever and memory stays put.
    assert_eq!(
        c.ask("CONFIG SET maxmemory-policy allkeys-lru").await,
        "+OK"
    );
    assert_eq!(c.fill("third", 20_000).await, 0);
    let evicted = c.info("evicted_keys").await;
    assert!(evicted > 15_000, "{evicted} evicted");
    let used = c.info("used_memory").await;
    assert!(used < limit + MB / 4, "{used} used with a limit of {limit}");
    let keys: u64 = c.ask("DBSIZE").await[1..].parse().unwrap();
    assert!((2000..7000).contains(&keys), "{keys} keys left");
    // What was written last is still there.
    assert_eq!(c.ask("GET third:19999").await.len(), 1000);

    // Lowering the limit evicts down to it.
    let lower = empty + MB;
    assert_eq!(c.ask(&format!("CONFIG SET maxmemory {lower}")).await, "+OK");
    assert_eq!(c.ask("PING").await, "+PONG");
    assert_eq!(c.ask("PING").await, "+PONG");
    let used = c.info("used_memory").await;
    assert!(used < lower + MB / 4, "{used} used with a limit of {lower}");

    // Freeing in the background gives the memory back too.
    assert_eq!(c.ask("CONFIG SET maxmemory 0").await, "+OK");
    assert_eq!(c.fill("fourth", 5000).await, 0);
    let before = c.info("used_memory").await;
    assert_eq!(c.ask("FLUSHALL ASYNC").await, "+OK");
    assert_eq!(c.ask("DBSIZE").await, ":0");
    let mut after = before;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        after = c.info("used_memory").await;
        if after + 4 * MB < before {
            break;
        }
    }
    assert!(after + 4 * MB < before, "{before} before, {after} after");

    // A background save gives back everything it used: nothing stays
    // attached to the keys and values it saved. (It once left a reference
    // count behind on each, 48 bytes a key, which here would be 1.9 MB.)
    assert_eq!(c.fill("fifth", 40_000).await, 0);
    let before = c.info("used_memory").await;
    assert_eq!(c.ask("BGSAVE").await, "+Background saving started");
    let mut persistence = String::new();
    for _ in 0..1_000 {
        persistence = c.ask("INFO persistence").await;
        if persistence.contains("rdb_bgsave_in_progress:0") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        persistence.contains("rdb_bgsave_in_progress:0")
            && persistence.contains("rdb_last_bgsave_status:ok"),
        "{persistence}"
    );
    assert!(dir.join("dump.rdb").metadata().unwrap().len() > 30 * MB);
    let mut after = c.info("used_memory").await;
    for _ in 0..50 {
        if after < before + MB / 2 {
            break;
        }
        // The writer thread may still be putting its buffers away.
        tokio::time::sleep(Duration::from_millis(20)).await;
        after = c.info("used_memory").await;
    }
    assert!(
        after < before + MB / 2,
        "{before} before the save, {after} after"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
