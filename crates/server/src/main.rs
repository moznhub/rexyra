use std::process::ExitCode;

use rexyra_server::config::{ArgsOutcome, Config, USAGE};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

fn main() -> ExitCode {
    let config = match Config::from_args(std::env::args().skip(1)) {
        Ok(ArgsOutcome::Run(config)) => config,
        Ok(ArgsOutcome::Help) => {
            print!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Ok(ArgsOutcome::Version) => {
            println!("rexyra-server {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("rexyra-server: {e}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();

    if !config.ignored.is_empty() {
        warn!(
            directives = %config.ignored.join(", "),
            "ignoring configuration directives this server does not support"
        );
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("io")
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            error!(error = %e, "could not start the I/O runtime");
            return ExitCode::FAILURE;
        }
    };

    runtime.block_on(async move {
        let endpoint = (config.bind.as_str(), config.port);
        let listener = match TcpListener::bind(endpoint).await {
            Ok(listener) => listener,
            Err(e) => {
                error!(bind = %config.bind, port = config.port, error = %e, "could not listen");
                return ExitCode::FAILURE;
            }
        };
        info!(
            bind = %config.bind,
            port = config.port,
            pid = std::process::id(),
            "ready to accept connections"
        );

        let shutdown = async {
            let _ = tokio::signal::ctrl_c().await;
            info!("Ctrl+C received, shutting down");
        };
        match rexyra_server::serve(listener, config, shutdown).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                error!(error = %e, "server stopped");
                ExitCode::FAILURE
            }
        }
    })
}
