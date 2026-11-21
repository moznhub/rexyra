use std::future::Future;
use std::io::IsTerminal;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Mutex;

use rexyra_server::config::{ArgsOutcome, Config, USAGE};
use rexyra_server::logfile::RotatingLog;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

#[cfg(windows)]
mod service;

/// Exit code after an internal error (a panic), on any thread. Exiting
/// without telling the service manager the service has stopped counts as a
/// crash, so a service set to restart on failure (as the installer sets it)
/// is started again, from the data on disk.
const EXIT_INTERNAL_ERROR: i32 = 70;

/// Makes any panic end the process, as an internal error does in Redis. A
/// panic means the server's state can no longer be trusted: carrying on
/// with the thread that failed gone would leave a server that accepts
/// connections but cannot run commands, or that quietly stops syncing its
/// append-only file.
fn exit_on_panic() {
    let print = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        error!(
            thread = thread.name().unwrap_or("unnamed"),
            "internal error, stopping so that the server can be restarted: {info}"
        );
        print(info);
        std::process::exit(EXIT_INTERNAL_ERROR);
    }));
}

fn main() -> ExitCode {
    exit_on_panic();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let as_service = args.iter().any(|arg| arg == "--service");
    let parsed = Config::from_args(&args);

    if as_service {
        // Under the service manager there is no terminal to print to, so
        // even a mistake in the configuration is reported through it.
        let config = match parsed {
            Ok(ArgsOutcome::Run(config)) => Ok(config),
            Ok(_) => Err("--service cannot be combined with that option".to_owned()),
            Err(e) => Err(e.to_string()),
        };
        #[cfg(windows)]
        return service::run(config, config_file(&args));
        #[cfg(not(windows))]
        {
            let _ = config;
            eprintln!("rexyra-server: --service is for Windows");
            return ExitCode::from(2);
        }
    }

    match parsed {
        Ok(ArgsOutcome::Run(config)) => {
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
                info!("Ctrl+C received, shutting down");
            };
            match run(config, shutdown, || {}) {
                Ok(()) => ExitCode::SUCCESS,
                Err(_) => ExitCode::FAILURE,
            }
        }
        Ok(ArgsOutcome::CreateConfig(path, config)) => create_config(&path, &config),
        Ok(ArgsOutcome::Help) => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        Ok(ArgsOutcome::Version) => {
            println!("rexyra-server {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("rexyra-server: {e}\n\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

/// The configuration file named on the command line, if there is one: the
/// first argument, once `--service` is set aside, unless it is an option.
#[cfg(windows)]
fn config_file(args: &[String]) -> Option<std::path::PathBuf> {
    args.iter()
        .find(|arg| *arg != "--service")
        .filter(|arg| !arg.starts_with('-'))
        .map(std::path::PathBuf::from)
}

/// `--create-config`: writes a starting configuration file, leaving one
/// that is already there alone so that an upgrade keeps what was changed.
fn create_config(path: &str, config: &Config) -> ExitCode {
    let path = Path::new(path);
    if path.exists() {
        println!("{} exists and was left as it is", path.display());
        return ExitCode::SUCCESS;
    }
    let written = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or(Ok(()), std::fs::create_dir_all)
        .and_then(|()| std::fs::write(path, config.starter_file()));
    match written {
        Ok(()) => {
            println!("wrote {}", path.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("rexyra-server: cannot write '{}': {e}", path.display());
            ExitCode::FAILURE
        }
    }
}

/// Where the log goes: the file named by `logfile`, or standard error.
fn start_logging(config: &Config) -> Result<(), String> {
    if config.logfile.is_empty() {
        // Colour is for someone watching a terminal; a pipe gets plain text.
        let colour = std::io::stderr().is_terminal()
            && std::env::var_os("NO_COLOR").is_none_or(|value| value.is_empty());
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_ansi(colour)
            .with_target(false)
            .init();
        return Ok(());
    }
    let mut file = RotatingLog::open(
        &config.logfile,
        config.logfile_max_size,
        config.logfile_max_files,
    )
    .map_err(|e| format!("cannot open the log file '{}': {e}", config.logfile))?;
    let _ = file.begin_run();
    tracing_subscriber::fmt()
        .with_writer(Mutex::new(file))
        .with_ansi(false)
        .with_target(false)
        .init();
    Ok(())
}

/// Runs the server until `shutdown` completes or a client sends SHUTDOWN.
/// `ready` is called once the data is loaded and connections are accepted.
/// An error is the reason the server could not start or stopped; it has
/// been written to the log, or to standard error if there is no log yet.
fn run(
    config: Config,
    shutdown: impl Future<Output = ()>,
    ready: impl FnOnce(),
) -> Result<(), String> {
    if let Err(why) = start_logging(&config) {
        eprintln!("rexyra-server: {why}");
        return Err(why);
    }

    // Before listening, so that no client is accepted by a server that
    // cannot save what it is given.
    if let Err(why) = rexyra_server::check_data_folder(&config.dir) {
        error!("{why}");
        if !config.logfile.is_empty() {
            eprintln!("rexyra-server: {why}");
        }
        return Err(why);
    }

    if !config.ignored.is_empty() {
        warn!(
            directives = %config.ignored.join(", "),
            "ignoring configuration directives this server does not support"
        );
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("io")
        .build()
        .map_err(|e| {
            error!(error = %e, "could not start the I/O runtime");
            format!("could not start the I/O runtime: {e}")
        })?;

    runtime.block_on(async move {
        let endpoint = (config.bind.as_str(), config.port);
        let listener = TcpListener::bind(endpoint).await.map_err(|e| {
            error!(bind = %config.bind, port = config.port, error = %e, "could not listen");
            format!("could not listen on {}:{}: {e}", config.bind, config.port)
        })?;
        info!(
            version = env!("CARGO_PKG_VERSION"),
            bind = %config.bind,
            port = config.port,
            pid = std::process::id(),
            "ready to accept connections"
        );

        rexyra_server::serve_with(listener, config, shutdown, ready)
            .await
            .map_err(|e| {
                error!(error = %e, "server stopped");
                format!("server stopped: {e}")
            })
    })
}
