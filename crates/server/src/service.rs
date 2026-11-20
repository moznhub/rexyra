//! Running under the Windows service manager (`--service`).
//!
//! The service manager starts the process, expects it to connect back and
//! report what state it is in, and asks it to stop through a callback
//! rather than Ctrl+C. Everything else is the same server.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use rexyra_server::config::Config;
use tokio::sync::Notify;
use tracing::info;
use windows_service::service::{
    ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus, ServiceType,
};
use windows_service::service_control_handler::{
    self, ServiceControlHandlerResult, ServiceStatusHandle,
};
use windows_service::{define_windows_service, service_dispatcher};

/// Windows ignores this name for a service that has a process to itself and
/// uses the one the service was registered under.
const NAME: &str = "Rexyra";

/// The exit code reported when the server could not start or failed. The
/// reason is in the log, and in [`ERROR_FILE`] beside the configuration.
const FAILED: u32 = 1;

/// Written beside the configuration file when the service cannot start, so
/// that a mistake in that file can be found without a terminal. Removed by
/// the next start that succeeds.
const ERROR_FILE: &str = "rexyra-startup-error.txt";

/// How often a start or a stop that is still going on is reported, and how
/// long the service manager is told to wait for the next report.
const PROGRESS_EVERY: Duration = Duration::from_secs(1);
const PROGRESS_WAIT: Duration = Duration::from_secs(20);

/// What `main` worked out, for the entry point the service manager calls,
/// which takes no arguments of ours.
struct Start {
    config: Result<Config, String>,
    config_file: Option<PathBuf>,
}

static START: OnceLock<Mutex<Option<Start>>> = OnceLock::new();

define_windows_service!(service_entry, service_main);

/// Hands the process over to the service manager and returns when the
/// service has stopped.
pub fn run(config: Result<Config, String>, config_file: Option<PathBuf>) -> ExitCode {
    let start = Start {
        config,
        config_file,
    };
    let _ = START.set(Mutex::new(Some(start)));
    match service_dispatcher::start(NAME, service_entry) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!(
                "rexyra-server: --service is for the Windows service manager and does not \
                 work from a terminal ({e}). Run the server without it."
            );
            ExitCode::FAILURE
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Starting,
    Running,
    Stopping,
    Stopped,
}

/// The state last reported to the service manager. Reports are made with
/// the lock held, so that a late progress report cannot follow "stopped".
struct Reporter {
    handle: ServiceStatusHandle,
    state: Mutex<(Phase, u32)>,
}

impl Reporter {
    fn set(&self, phase: Phase, exit_code: ServiceExitCode) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.0 == Phase::Stopped {
            return;
        }
        *state = (phase, 0);
        self.report(phase, 0, exit_code);
    }

    /// Tells the service manager that a start or stop is still under way.
    /// Returns whether there is anything left to report.
    fn progress(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (phase, checkpoint) = *state;
        if matches!(phase, Phase::Starting | Phase::Stopping) {
            state.1 = checkpoint + 1;
            self.report(phase, checkpoint + 1, ServiceExitCode::Win32(0));
        }
        phase != Phase::Stopped
    }

    fn report(&self, phase: Phase, checkpoint: u32, exit_code: ServiceExitCode) {
        let pending = matches!(phase, Phase::Starting | Phase::Stopping);
        let status = ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: match phase {
                Phase::Starting => ServiceState::StartPending,
                Phase::Running => ServiceState::Running,
                Phase::Stopping => ServiceState::StopPending,
                Phase::Stopped => ServiceState::Stopped,
            },
            // "Pre-shutdown" rather than "shutdown": Windows then waits for
            // the service, minutes if need be, before it shuts down, which
            // a snapshot of a large data set can need.
            controls_accepted: if phase == Phase::Running {
                ServiceControlAccept::STOP | ServiceControlAccept::PRESHUTDOWN
            } else {
                ServiceControlAccept::empty()
            },
            exit_code,
            checkpoint,
            wait_hint: if pending {
                PROGRESS_WAIT
            } else {
                Duration::ZERO
            },
            process_id: None,
        };
        // There is nobody to tell if the service manager cannot be told.
        let _ = self.handle.set_service_status(status);
    }
}

/// Called by the service manager, on a thread of its own, once the process
/// has connected to it.
fn service_main(_arguments: Vec<OsString>) {
    let Some(start) = START
        .get()
        .and_then(|start| start.lock().unwrap_or_else(PoisonError::into_inner).take())
    else {
        return;
    };

    let stop = Arc::new(Notify::new());
    let asked = Arc::clone(&stop);
    let registered = service_control_handler::register(NAME, move |control| match control {
        ServiceControl::Stop | ServiceControl::Preshutdown | ServiceControl::Shutdown => {
            // Kept until the server waits for it, should it not be yet.
            asked.notify_one();
            ServiceControlHandlerResult::NoError
        }
        ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
        _ => ServiceControlHandlerResult::NotImplemented,
    });
    let Ok(handle) = registered else {
        return;
    };
    let reporter = Arc::new(Reporter {
        handle,
        state: Mutex::new((Phase::Starting, 0)),
    });
    reporter.set(Phase::Starting, ServiceExitCode::Win32(0));

    // Loading the data at start and saving it at stop can take a while;
    // the service manager gives up on a service it does not hear from.
    let ticking = Arc::clone(&reporter);
    std::thread::spawn(move || {
        while ticking.progress() {
            std::thread::sleep(PROGRESS_EVERY);
        }
    });

    let error_file = start
        .config_file
        .as_deref()
        .and_then(Path::parent)
        .map(|folder| folder.join(ERROR_FILE));

    let outcome = start.config.and_then(|config| {
        let shutdown = {
            let reporter = Arc::clone(&reporter);
            async move {
                stop.notified().await;
                info!("the service manager asked the server to stop");
                reporter.set(Phase::Stopping, ServiceExitCode::Win32(0));
            }
        };
        let ready = {
            let reporter = Arc::clone(&reporter);
            let error_file = error_file.clone();
            move || {
                if let Some(stale) = error_file {
                    let _ = std::fs::remove_file(stale);
                }
                reporter.set(Phase::Running, ServiceExitCode::Win32(0));
            }
        };
        crate::run(config, shutdown, ready)
    });

    let exit_code = match outcome {
        Ok(()) => ServiceExitCode::Win32(0),
        Err(why) => {
            if let Some(file) = error_file {
                let _ = std::fs::write(
                    file,
                    format!("The Rexyra service could not start, or stopped:\r\n\r\n{why}\r\n"),
                );
            }
            ServiceExitCode::ServiceSpecific(FAILED)
        }
    };
    reporter.set(Phase::Stopped, exit_code);
}
