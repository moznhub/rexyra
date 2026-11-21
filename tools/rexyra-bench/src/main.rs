//! rexyra-bench: measures command latency, throughput and memory of rexyra-server
//! before, during and after a BGSAVE, and (`ops`) the throughput of single
//! commands. Plain threads and blocking sockets; every latency sample is
//! kept and percentiles are exact.

mod load;
mod ops;
mod resp;
mod run;
mod scenario;
mod server;
mod stats;
mod summary;

use std::process::ExitCode;

const USAGE: &str = "\
usage:
  rexyra-bench list
  rexyra-bench run --scenario <name> [--runs 3] [--first-run 1] [--port 6400]
               [--save-delay-us N] [--label TEXT] [--out docs/benchmarks/bgsave/raw]
               [--server target/release/rexyra-server.exe] [--data-root target/bench-data]
               [--clients 50] [--get-percent 50] [--warmup-secs 1] [--baseline-secs 5]
               [--after-secs 3] [--monitor-ms 10] [--keep-data] [--no-toucher]
  rexyra-bench summarize [--raw docs/benchmarks/bgsave/raw] [--out FILE]
  rexyra-bench ops [--commands SET,GET,LPUSH] [--clients 50] [--pipeline 16] [--seconds 5]
               [--warmup-secs 1] [--runs 3] [--notify FLAGS] [--label TEXT] [--port 6400]
               [--server target/release/rexyra-server.exe] [--data-root target/bench-data]";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("run") => run::Options::parse(&args[1..]).and_then(|o| run::run_scenario(&o)),
        Some("summarize") => summary::main(&args[1..]),
        Some("ops") => ops::Options::parse(&args[1..]).and_then(|o| ops::run(&o)),
        Some("list") => {
            run::list();
            Ok(())
        }
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("rexyra-bench: {e}");
            ExitCode::FAILURE
        }
    }
}
