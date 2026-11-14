//! The Oxhaven server binary.

use std::process::ExitCode;
use std::time::Duration;

use oxhaven_server::session::{random_psk, AuthHandler, AuthHandlers, DEFAULT_TOKEN_EXPIRE_MS};
use oxhaven_server::update_graph::DEFAULT_TARGET_CYCLE_DURATION;
use oxhaven_server::{app, serve, ServerConfig, ServerState};
use tokio::net::TcpListener;

const USAGE: &str = "\
Usage: oxhaven-server [options]

  --host <address>          address to listen on (default 0.0.0.0)
  --port <port>             port to listen on (default 10000)
  --auth <handlers>         comma-separated list of psk and anonymous (default psk)
  --psk <key>               pre-shared key; also read from OXHAVEN_PSK; random if absent
  --token-expire-ms <ms>    session token lifetime (default 300000)
  --cycle-ms <ms>           target update graph cycle duration (default 1000)
  --demo-tables             offer the application ide_app with the tables ide_static and
                            ide_ticking, which the web IDE acceptance test expects
";

struct Args {
    host: String,
    port: u16,
    auth: String,
    psk: Option<String>,
    token_expire_ms: i64,
    cycle: Duration,
    demo_tables: bool,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut parsed = Args {
        host: "0.0.0.0".to_string(),
        port: 10000,
        auth: "psk".to_string(),
        psk: std::env::var("OXHAVEN_PSK").ok(),
        token_expire_ms: DEFAULT_TOKEN_EXPIRE_MS,
        cycle: DEFAULT_TARGET_CYCLE_DURATION,
        demo_tables: false,
    };
    while let Some(flag) = args.next() {
        let mut value = || {
            args.next()
                .ok_or_else(|| format!("{flag} needs a value\n\n{USAGE}"))
        };
        match flag.as_str() {
            "--host" => parsed.host = value()?,
            "--port" => parsed.port = value()?.parse().map_err(|e| format!("--port: {e}"))?,
            "--auth" => parsed.auth = value()?,
            "--psk" => parsed.psk = Some(value()?),
            "--token-expire-ms" => {
                parsed.token_expire_ms = value()?
                    .parse()
                    .map_err(|e| format!("--token-expire-ms: {e}"))?
            }
            "--cycle-ms" => {
                parsed.cycle = value()?
                    .parse()
                    .map(Duration::from_millis)
                    .map_err(|e| format!("--cycle-ms: {e}"))?
            }
            "--demo-tables" => parsed.demo_tables = true,
            "-h" | "--help" => return Err(USAGE.to_string()),
            other => return Err(format!("unknown option {other}\n\n{USAGE}")),
        }
    }
    Ok(parsed)
}

fn auth_handlers(args: &Args) -> Result<AuthHandlers, String> {
    let mut handlers = Vec::new();
    for name in args.auth.split(',').map(str::trim) {
        match name {
            "anonymous" => {
                tracing::warn!("Anonymous authentication is enabled. This is not recommended!");
                handlers.push(AuthHandler::Anonymous);
            }
            "psk" => {
                let key = match args.psk.as_deref().map(str::trim) {
                    Some(key) if !key.is_empty() => key.to_string(),
                    _ => random_psk(),
                };
                tracing::warn!(
                    "Superuser access through pre-shared key is enabled - use {key} to connect"
                );
                handlers.push(AuthHandler::psk(&key)?);
            }
            other => return Err(format!("unknown auth handler {other}\n\n{USAGE}")),
        }
    }
    AuthHandlers::new(handlers)
}

async fn run(args: Args) -> Result<(), String> {
    let state = ServerState::new(ServerConfig {
        auth_handlers: auth_handlers(&args)?,
        token_expire_ms: args.token_expire_ms,
        target_cycle_duration: Some(args.cycle),
    })?;
    if args.demo_tables {
        let application = app::demo::install(&state.applications, &state.op_context())?;
        tracing::info!(
            "Application {} offers {}",
            application.id(),
            application.field_names().join(", ")
        );
    }
    let listener = TcpListener::bind((args.host.as_str(), args.port))
        .await
        .map_err(|e| format!("cannot listen on {}:{}: {e}", args.host, args.port))?;
    let address = listener.local_addr().map_err(|e| e.to_string())?;
    tracing::info!("Listening on {address}");
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Shutting down");
    };
    serve(state, listener, shutdown)
        .await
        .map_err(|e| format!("server failed: {e}"))
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let outcome = match parse_args(std::env::args().skip(1)) {
        Ok(args) => run(args).await,
        Err(message) => Err(message),
    };
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}
