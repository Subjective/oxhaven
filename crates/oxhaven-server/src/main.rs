//! The Oxhaven server binary.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use oxhaven_server::console::logs::{self, LogBuffer};
use oxhaven_server::session::{random_psk, AuthHandler, AuthHandlers, DEFAULT_TOKEN_EXPIRE_MS};
use oxhaven_server::update_graph::DEFAULT_TARGET_CYCLE_DURATION;
use oxhaven_server::web::WebRoot;
use oxhaven_server::{app, serve, ServerConfig, ServerState};
use tokio::net::TcpListener;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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
  --console <type>          python or none: the language of the console (default python if
                            the server was built with the python feature, otherwise none)
  --python-home <directory> the virtual environment or Python installation whose packages
                            the Python console imports (default: the directory VIRTUAL_ENV
                            names, otherwise the Python the server was built with); its
                            Python version must be the one the server is linked against
  --python-path <paths>     directories the Python console searches for modules before
                            the environment's own, separated as in PATH; may be repeated
  --web-root <directory>    the web IDE's files, served at /ide/, /iframe/ and /jsapi/;
                            scripts/extract-web-root.sh makes the directory (default: none)
";

struct Args {
    host: String,
    port: u16,
    auth: String,
    psk: Option<String>,
    token_expire_ms: i64,
    cycle: Duration,
    demo_tables: bool,
    console: Option<String>,
    python_home: Option<PathBuf>,
    python_path: Vec<PathBuf>,
    web_root: Option<PathBuf>,
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
        console: None,
        python_home: None,
        python_path: Vec::new(),
        web_root: None,
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
            "--console" => parsed.console = Some(value()?),
            "--python-home" => parsed.python_home = Some(PathBuf::from(value()?)),
            "--python-path" => {
                let paths = value()?;
                parsed.python_path.extend(std::env::split_paths(&paths));
            }
            "--web-root" => parsed.web_root = Some(PathBuf::from(value()?)),
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

/// Gives the server the console `--console` names.
fn start_console(args: &Args, state: &mut ServerState) -> Result<(), String> {
    let default = if cfg!(feature = "python") {
        "python"
    } else {
        "none"
    };
    let console = args.console.as_deref().unwrap_or(default);
    if console != "python" && (args.python_home.is_some() || !args.python_path.is_empty()) {
        return Err(format!(
            "--python-home and --python-path are for the Python console, and the console \
             is {console}"
        ));
    }
    match console {
        "none" => Ok(()),
        #[cfg(feature = "python")]
        "python" => {
            use oxhaven_server::console::python::{self, env};
            let environment = env::PythonEnv {
                home: args.python_home.clone().or_else(env::virtual_env),
                path: args.python_path.clone(),
            };
            let interpreter = env::initialize(&environment)?;
            python::start(state)?;
            tracing::info!(
                "Python console: Python {}, packages from {}",
                interpreter.version,
                interpreter.prefix
            );
            Ok(())
        }
        #[cfg(not(feature = "python"))]
        "python" => {
            let _ = state;
            Err(
                "this server was built without the Python console; build it with \
                 `--features python` (docs/CONSOLE.md)"
                    .to_string(),
            )
        }
        other => Err(format!("unknown console type {other}\n\n{USAGE}")),
    }
}

async fn run(args: Args, logs: Arc<LogBuffer>) -> Result<(), String> {
    let web_root = match &args.web_root {
        Some(dir) => Some(Arc::new(WebRoot::open(dir)?)),
        None => None,
    };
    let mut state = ServerState::new(ServerConfig {
        auth_handlers: auth_handlers(&args)?,
        token_expire_ms: args.token_expire_ms,
        target_cycle_duration: Some(args.cycle),
    })?;
    state.logs = logs;
    if args.demo_tables {
        let application = app::demo::install(&state.applications, &state.op_context())?;
        tracing::info!(
            "Application {} offers {}",
            application.id(),
            application.field_names().join(", ")
        );
    }
    start_console(&args, &mut state)?;
    state.web_root = web_root;
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
    // The log goes to standard error and, as in Java, into the buffer that clients read
    // with ConsoleService/SubscribeToLogs: from INFO up, the root level of the Java
    // server's logging configuration.
    let logs = Arc::new(LogBuffer::new());
    tracing_subscriber::registry()
        .with(LevelFilter::INFO)
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(logs::layer(logs.clone()))
        .init();
    let outcome = match parse_args(std::env::args().skip(1)) {
        Ok(args) => run(args, logs).await,
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
