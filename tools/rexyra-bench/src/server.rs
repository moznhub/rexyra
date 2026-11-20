//! Starting and stopping rexyra-server processes. A process is only ever stopped
//! through the handle obtained when it was started.

use crate::resp::{Conn, Reply};
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub struct ServerProc {
    child: Option<Child>,
    port: u16,
}

/// Fails if something already answers on `port`, so a stray server is never
/// mistaken for the one this tool starts.
pub fn ensure_port_free(port: u16) -> io::Result<()> {
    match Conn::connect(port) {
        Ok(_) => Err(io::Error::other(format!(
            "something is already listening on 127.0.0.1:{port}; pick another --port"
        ))),
        Err(_) => Ok(()),
    }
}

impl ServerProc {
    /// Starts `exe` with automatic saves off. `dbfilename` overrides the
    /// snapshot file name (default `dump.rdb`).
    pub fn start(
        exe: &Path,
        port: u16,
        dir: &Path,
        dbfilename: Option<&str>,
        log: &Path,
    ) -> io::Result<Self> {
        ensure_port_free(port)?;
        let log_file = OpenOptions::new().create(true).append(true).open(log)?;
        let mut command = Command::new(exe);
        command
            .arg("--port")
            .arg(port.to_string())
            .arg("--dir")
            .arg(dir)
            .arg("--save")
            .arg("");
        if let Some(name) = dbfilename {
            command.arg("--dbfilename").arg(name);
        }
        command
            .stdin(Stdio::null())
            .stdout(log_file.try_clone()?)
            .stderr(log_file);
        let child = command.spawn()?;
        Ok(Self {
            child: Some(child),
            port,
        })
    }

    pub fn pid(&self) -> u32 {
        self.child.as_ref().map_or(0, Child::id)
    }

    /// Waits until the server answers `PING` with `PONG`; returns how long
    /// that took (for a server loading a snapshot: the load time, roughly).
    pub fn wait_ready(&mut self, timeout: Duration) -> io::Result<Duration> {
        let started = Instant::now();
        loop {
            if let Some(child) = &mut self.child {
                if let Some(status) = child.try_wait()? {
                    return Err(io::Error::other(format!(
                        "the server exited during start-up ({status})"
                    )));
                }
            }
            if let Ok(mut conn) = Conn::connect(self.port) {
                conn.set_timeout(Some(timeout))?;
                if let Ok(Reply::Simple(s)) = conn.call(&[b"PING"]) {
                    if s == "PONG" {
                        return Ok(started.elapsed());
                    }
                }
            }
            if started.elapsed() > timeout {
                return Err(io::Error::other("the server did not become ready in time"));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// `SHUTDOWN NOSAVE`, then waits for the process to exit. If it does not
    /// within 60 s it is killed through its handle.
    pub fn shutdown(mut self) -> io::Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        if let Ok(mut conn) = Conn::connect(self.port) {
            conn.set_timeout(Some(Duration::from_secs(10)))?;
            let _ = conn.call(&[b"SHUTDOWN", b"NOSAVE"]);
        }
        let started = Instant::now();
        loop {
            if child.try_wait()?.is_some() {
                return Ok(());
            }
            if started.elapsed() > Duration::from_secs(60) {
                child.kill()?;
                child.wait()?;
                return Err(io::Error::other(
                    "the server did not exit after SHUTDOWN NOSAVE; killed it through its process handle",
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for ServerProc {
    fn drop(&mut self) {
        // Only reached on an error path: stop the process this tool started.
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
