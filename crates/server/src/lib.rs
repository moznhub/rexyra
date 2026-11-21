//! Redis-compatible server for Windows.
//!
//! Network I/O runs on a Tokio runtime (IOCP underneath on Windows); commands
//! run on a single dedicated thread — see [`executor`].

pub mod config;
mod connection;
pub mod executor;
pub mod logfile;

use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rexyra_commands::{aof, persistence, MemoryMeter, Server};
use rexyra_engine::Keyspace;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

pub use config::Config;
use executor::Executor;

/// Counts every byte the process allocates, which is what `maxmemory` is
/// compared against and what `INFO` reports as `used_memory`. (The Lua
/// interpreter allocates through the C library and is not included.)
#[global_allocator]
static ALLOCATOR: cap::Cap<std::alloc::System> = cap::Cap::new(std::alloc::System, usize::MAX);

/// Bytes currently allocated by the process.
pub fn allocated_bytes() -> u64 {
    ALLOCATOR.allocated() as u64
}

/// Accepts connections on `listener` until `shutdown` completes.
pub async fn serve(
    listener: TcpListener,
    config: Config,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    serve_with(listener, config, shutdown, || {}).await
}

/// [`serve`], calling `ready` once the data is loaded and connections are
/// about to be accepted.
pub async fn serve_with(
    listener: TcpListener,
    config: Config,
    shutdown: impl Future<Output = ()>,
    ready: impl FnOnce(),
) -> io::Result<()> {
    // `CONFIG SET maxclients` changes this while the server runs.
    let maxclients = Arc::new(AtomicUsize::new(config.maxclients));
    let mut server = Server::default();
    server.databases = config.databases;
    server.bind = config.bind;
    // The port actually bound, which differs from the configured one when
    // that is 0.
    server.port = listener.local_addr()?.port();
    server.maxclients = Arc::clone(&maxclients);
    server.config_file = config.config_file;
    server.slowlog_slower_than = config.slowlog_log_slower_than;
    server.slowlog_max_len = config.slowlog_max_len;
    server.busy_reply_threshold = config.busy_reply_threshold;
    server.dir = config.dir;
    server.dbfilename = config.dbfilename;
    server.save_points = config.save_points;
    server.aof_settings = config.aof;
    server.maxmemory = config.maxmemory;
    server.maxmemory_policy = config.maxmemory_policy;
    server.maxmemory_samples = config.maxmemory_samples;
    server.lfu_log_factor = config.lfu_log_factor;
    server.lfu_decay_time = config.lfu_decay_time;
    server.meter = MemoryMeter::Allocator(allocated_bytes);
    server.protected_mode = config.protected_mode;
    server.enable_debug_command = config.enable_debug_command;
    for user in &config.users {
        server
            .acl
            .set_user(&user[0], &user[1..])
            .map_err(|why| io::Error::other(format!("user '{}': {why}", user[0])))?;
    }
    // After the `user` lines, so that it wins over a `user default` line,
    // and so that protection reflects the final state.
    if !config.requirepass.is_empty() {
        server.set_requirepass(&config.requirepass);
    }
    server.refresh_protection();
    let loopback_only = Arc::clone(&server.loopback_only);

    // A save that was cut short by a crash or a kill left its unfinished
    // file behind; nothing will ever complete it.
    for stale in persistence::remove_stale_temporaries(&server) {
        warn!(file = %stale.display(), "removed the unfinished file of an interrupted save");
    }

    // Start from the snapshot, if there is one. A snapshot that cannot be
    // read stops the server rather than starting empty and later saving
    // over it.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64);
    let mut keyspace = if server.aof_settings.enabled {
        // The append-only file is the more complete record, so it wins.
        let (keyspace, loaded) = aof::load(&mut server, now).map_err(io::Error::other)?;
        if loaded.truncated > 0 {
            warn!(
                bytes = loaded.truncated,
                "the append-only file ended in the middle of a command and was cut back"
            );
        }
        info!(
            base_keys = loaded.base_keys,
            commands = loaded.commands,
            created = loaded.created,
            "loaded append-only file"
        );
        keyspace
    } else {
        match persistence::load(&mut server, now).map_err(io::Error::other)? {
            Some((keyspace, loaded)) => {
                info!(
                    keys = loaded.keys,
                    expired = loaded.expired,
                    libraries = loaded.libraries,
                    file = %persistence::snapshot_path(&server).display(),
                    "loaded snapshot"
                );
                keyspace
            }
            None => Keyspace::new(server.databases),
        }
    };
    keyspace.set_lfu(server.lfu_log_factor, server.lfu_decay_time);
    // Automatic saves count their interval from start-up.
    server.last_save = now / 1000;
    let (executor, stopped) = Executor::spawn(server, keyspace)?;
    tokio::pin!(stopped);
    let next_id = AtomicU64::new(1);
    let active = Arc::new(AtomicUsize::new(0));
    tokio::pin!(shutdown);

    // Drives active expiry and incremental rehashing, ten times a second
    // like Redis's default `hz`.
    let mut maintenance = tokio::time::interval(Duration::from_millis(100));
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    ready();
    loop {
        let (mut stream, addr) = tokio::select! {
            _ = &mut shutdown => {
                // Asked to exit from outside (Ctrl+C): keep what changed.
                match executor.save_if_dirty().await {
                    Ok(true) => info!("saved snapshot before exit"),
                    Ok(false) => {}
                    Err(e) => warn!(error = %e, "could not save snapshot before exit"),
                }
                return Ok(());
            }
            // A client sent SHUTDOWN; any save has been done.
            _ = &mut stopped => return Ok(()),
            _ = maintenance.tick() => {
                executor.tick();
                continue;
            }
            accepted = listener.accept() => match accepted {
                Ok(pair) => pair,
                Err(e) => {
                    // Usually transient (e.g. out of handles). Back off briefly
                    // rather than spinning.
                    warn!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
        };

        // Protected mode: with no password set, only this machine may
        // connect, so that a server bound to a public address by mistake is
        // not open to everyone.
        if loopback_only.load(Ordering::Relaxed) && !addr.ip().to_canonical().is_loopback() {
            tokio::spawn(async move {
                let _ = stream.write_all(PROTECTED_MODE_DENIAL).await;
            });
            continue;
        }

        let slot = ClientSlot::acquire(&active, maxclients.load(Ordering::Relaxed));
        let id = next_id.fetch_add(1, Ordering::Relaxed);
        let executor = executor.clone();

        tokio::spawn(async move {
            let Some(_slot) = slot else {
                let _ = stream
                    .write_all(b"-ERR max number of clients reached\r\n")
                    .await;
                return;
            };
            // Replies are small and latency-sensitive; never wait for Nagle.
            let _ = stream.set_nodelay(true);

            let laddr = stream
                .local_addr()
                .map(|local| local.to_string())
                .unwrap_or_default();
            let channels = executor.connect(id, addr.to_string(), laddr);
            if let Err(e) = connection::serve(stream, id, &executor, channels).await {
                debug!(client = id, error = %e, "connection ended with error");
            }
            executor.disconnect(id);
        });
    }
}

const PROTECTED_MODE_DENIAL: &[u8] = b"-DENIED The server is running in protected mode: no \
password is set for the default user, so connections are only accepted from the machine it \
runs on. To connect from other machines, set a password ('CONFIG SET requirepass <password>' \
from the server's machine, the 'requirepass' directive in its configuration file, or the \
'--requirepass' option), or, if the network is trusted, turn protected mode off ('CONFIG SET \
protected-mode no', the 'protected-mode no' directive, or '--protected-mode no').\r\n";

/// Counts a connection against `maxclients` for as long as it is alive.
struct ClientSlot(Arc<AtomicUsize>);

impl ClientSlot {
    fn acquire(active: &Arc<AtomicUsize>, max: usize) -> Option<ClientSlot> {
        if active.fetch_add(1, Ordering::AcqRel) >= max {
            active.fetch_sub(1, Ordering::AcqRel);
            None
        } else {
            Some(ClientSlot(Arc::clone(active)))
        }
    }
}

impl Drop for ClientSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
