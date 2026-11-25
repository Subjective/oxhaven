//! The Oxhaven server binary.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use oxhaven_column::parallel;
use oxhaven_server::console::logs::{self, LogBuffer};
use oxhaven_server::session::{random_psk, AuthHandler, AuthHandlers, DEFAULT_TOKEN_EXPIRE_MS};
use oxhaven_server::storage::StorageRoot;
use oxhaven_server::transport::{serve_tls, ClientAuth, TlsAcceptor, TlsConfig};
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
  --port <port>             port to listen on (default 10000, or 443 with --tls-cert)
  --auth <handlers>         comma-separated list of psk and anonymous (default psk)
  --psk <key>               pre-shared key; also read from OXHAVEN_PSK; random if absent
  --token-expire-ms <ms>    session token lifetime (default 300000)
  --cycle-ms <ms>           target update graph cycle duration (default 1000)
  --threads <count>         threads one table operation may use for a filter, a formula
                            or a read of many rows (default: the value of OXHAVEN_THREADS,
                            otherwise the number of cores available)
  --demo-tables             offer the application ide_app with the tables ide_static and
                            ide_ticking, which the web IDE acceptance test expects
  --app-dir <directory>     start the applications of the .app files in the directory: each
                            names Python scripts that run before the server accepts
                            clients and set the application's fields (deephaven.appmode);
                            a script that fails keeps the server from starting
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
  --storage-root <directory>  where the web IDE's layouts and notebooks are kept; created
                            if missing (default: none, and the IDE can save nothing)
  --tls-cert <file>         serve TLS with the PEM certificates of the file, the server's
                            first and then those that chain it to a root (Java's
                            ssl.identity.certChainPath); needs --tls-key and a server built
                            with the tls feature (docs/TLS.md)
  --tls-key <file>          the PEM private key of the first certificate, without a
                            password (ssl.identity.privateKeyPath)
  --tls-trust <file>        PEM certificates that a client's certificate must chain to
                            (ssl.trust.path)
  --tls-client-auth <mode>  NONE, WANTED or NEEDED: whether clients are asked for a
                            certificate (ssl.clientAuthentication; default NONE)
";

struct Args {
    host: String,
    /// `None`: Java's default, which depends on TLS ([`Args::port`]).
    port: Option<u16>,
    auth: String,
    psk: Option<String>,
    token_expire_ms: i64,
    cycle: Duration,
    threads: Option<NonZeroUsize>,
    demo_tables: bool,
    app_dir: Option<PathBuf>,
    console: Option<String>,
    python_home: Option<PathBuf>,
    python_path: Vec<PathBuf>,
    web_root: Option<PathBuf>,
    storage_root: Option<PathBuf>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    tls_trust: Option<PathBuf>,
    tls_client_auth: Option<ClientAuth>,
}

impl Args {
    /// The port: the one given, otherwise 443 with TLS and 10000 without.
    ///
    /// Behavior: `io.deephaven.server.jetty.JettyConfig#port` (`DEFAULT_SSL_PORT`,
    /// `DEFAULT_PLAINTEXT_PORT`)
    fn port(&self) -> u16 {
        self.port
            .unwrap_or(if self.tls_cert.is_some() { 443 } else { 10000 })
    }

    /// The TLS the server serves, if `--tls-cert` asks for it. Half an identity is refused,
    /// and so are the trust options without one.
    ///
    /// Behavior: `io.deephaven.server.runner.MainHelper#parseSSLConfig`
    fn tls(&self) -> Result<Option<TlsConfig>, String> {
        let (cert_chain, private_key) = match (&self.tls_cert, &self.tls_key) {
            (Some(cert_chain), Some(private_key)) => (cert_chain.clone(), private_key.clone()),
            (None, None) => {
                if self.tls_trust.is_some() || self.tls_client_auth.is_some() {
                    return Err(
                        "--tls-trust and --tls-client-auth need --tls-cert and --tls-key"
                            .to_string(),
                    );
                }
                return Ok(None);
            }
            _ => return Err("Must specify `--tls-cert` and `--tls-key`".to_string()),
        };
        let config = TlsConfig {
            cert_chain,
            private_key,
            trust: self.tls_trust.clone(),
            client_auth: self.tls_client_auth.unwrap_or_default(),
        };
        config.check()?;
        Ok(Some(config))
    }
}

/// The process's allocator when built with the `mimalloc` feature (on by default). It reuses
/// freed memory instead of returning it to the system and taking it again, which with the
/// system allocator was about a sixth of the server's processor time (docs/BENCHMARK.md,
/// "After task 9.13").
#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut parsed = Args {
        host: "0.0.0.0".to_string(),
        port: None,
        auth: "psk".to_string(),
        psk: std::env::var("OXHAVEN_PSK").ok(),
        token_expire_ms: DEFAULT_TOKEN_EXPIRE_MS,
        cycle: DEFAULT_TARGET_CYCLE_DURATION,
        threads: None,
        demo_tables: false,
        app_dir: None,
        console: None,
        python_home: None,
        python_path: Vec::new(),
        web_root: None,
        storage_root: None,
        tls_cert: None,
        tls_key: None,
        tls_trust: None,
        tls_client_auth: None,
    };
    while let Some(flag) = args.next() {
        let mut value = || {
            args.next()
                .ok_or_else(|| format!("{flag} needs a value\n\n{USAGE}"))
        };
        match flag.as_str() {
            "--host" => parsed.host = value()?,
            "--port" => parsed.port = Some(value()?.parse().map_err(|e| format!("--port: {e}"))?),
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
            "--threads" => {
                let count = value()?.parse().map_err(|e| format!("--threads: {e}"))?;
                parsed.threads = Some(count)
            }
            "--demo-tables" => parsed.demo_tables = true,
            "--app-dir" => parsed.app_dir = Some(PathBuf::from(value()?)),
            "--console" => parsed.console = Some(value()?),
            "--python-home" => parsed.python_home = Some(PathBuf::from(value()?)),
            "--python-path" => {
                let paths = value()?;
                parsed.python_path.extend(std::env::split_paths(&paths));
            }
            "--web-root" => parsed.web_root = Some(PathBuf::from(value()?)),
            "--storage-root" => parsed.storage_root = Some(PathBuf::from(value()?)),
            "--tls-cert" => parsed.tls_cert = Some(PathBuf::from(value()?)),
            "--tls-key" => parsed.tls_key = Some(PathBuf::from(value()?)),
            "--tls-trust" => parsed.tls_trust = Some(PathBuf::from(value()?)),
            "--tls-client-auth" => {
                let mode =
                    ClientAuth::parse(&value()?).map_err(|e| format!("--tls-client-auth: {e}"))?;
                parsed.tls_client_auth = Some(mode)
            }
            "-h" | "--help" => return Err(USAGE.to_string()),
            other => return Err(format!("unknown option {other}\n\n{USAGE}")),
        }
    }
    Ok(parsed)
}

/// Gives the engine the thread count `--threads` names, if it names one, and returns the
/// count table operations use from now on.
fn use_threads(args: &Args) -> usize {
    if let Some(count) = args.threads {
        parallel::set_threads(count.get());
    }
    parallel::threads()
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
    // the certificate and key are read before anything starts
    let tls = match args.tls()? {
        Some(config) => Some(Arc::new(TlsAcceptor::new(&config)?)),
        None => None,
    };
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
    tracing::info!("Table operations use up to {} threads", use_threads(&args));
    if let Some(directory) = &args.storage_root {
        let storage = StorageRoot::open(directory)?;
        tracing::info!("Storage in {}", storage.path().display());
        state.storage = Some(Arc::new(storage));
    }
    if args.demo_tables {
        let application = app::demo::install(&state.applications, &state.op_context())?;
        tracing::info!(
            "Application {} offers {}",
            application.id(),
            application.field_names().join(", ")
        );
    }
    start_console(&args, &mut state)?;
    if let Some(directory) = args.app_dir.clone() {
        // as in Java, before any client is accepted; a script may block
        let (applications, console) = (state.applications.clone(), state.console.clone());
        tokio::task::spawn_blocking(move || {
            app::script::load(&directory, &applications, console.as_ref())
        })
        .await
        .map_err(|e| format!("the applications could not be started: {e}"))??;
    }
    state.web_root = web_root;
    let port = args.port();
    let listener = TcpListener::bind((args.host.as_str(), port))
        .await
        .map_err(|e| format!("cannot listen on {}:{port}: {e}", args.host))?;
    let address = listener.local_addr().map_err(|e| e.to_string())?;
    let scheme = if tls.is_some() { "https" } else { "http" };
    tracing::info!("Listening on {address} ({scheme})");
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Shutting down");
    };
    match tls {
        None => serve(state, listener, shutdown).await,
        Some(tls) => serve_tls(state, listener, tls, shutdown).await,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Args, String> {
        parse_args(args.iter().map(|arg| arg.to_string()))
    }

    #[test]
    fn threads_is_a_positive_count_and_absent_by_default() {
        assert_eq!(parse(&[]).ok().and_then(|args| args.threads), None);
        let args = parse(&["--port", "10888", "--threads", "3"]).ok().unwrap();
        assert_eq!(
            (args.port(), args.threads.map(NonZeroUsize::get)),
            (10888, Some(3))
        );
        for (bad, message) in [
            (
                &["--threads", "0"][..],
                "--threads: number would be zero for non-zero type",
            ),
            (
                &["--threads", "-2"][..],
                "--threads: invalid digit found in string",
            ),
            (
                &["--threads", "many"][..],
                "--threads: invalid digit found in string",
            ),
        ] {
            assert_eq!(parse(bad).err().as_deref(), Some(message));
        }
        let missing = parse(&["--threads"]).err().unwrap();
        assert!(missing.starts_with("--threads needs a value"), "{missing}");
        assert!(USAGE.contains("--threads <count>"));
    }

    #[test]
    fn tls_options_name_a_whole_identity_and_move_the_default_port() {
        let plain = parse(&[]).ok().unwrap();
        assert_eq!((plain.port(), plain.tls()), (10000, Ok(None)));
        let tls = parse(&["--tls-cert", "chain.pem", "--tls-key", "key.pem"])
            .ok()
            .unwrap();
        assert_eq!(tls.port(), 443);
        let config = tls.tls().unwrap().unwrap();
        assert_eq!(
            (config.cert_chain, config.private_key, config.trust),
            ("chain.pem".into(), "key.pem".into(), None)
        );
        assert_eq!(config.client_auth, ClientAuth::None);
        let given = parse(&["--tls-cert", "c", "--tls-key", "k", "--port", "8443"]);
        assert_eq!(given.ok().unwrap().port(), 8443);
        for half in [&["--tls-cert", "c"][..], &["--tls-key", "k"][..]] {
            let refused = parse(half).ok().unwrap().tls().unwrap_err();
            assert_eq!(refused, "Must specify `--tls-cert` and `--tls-key`");
        }
        let alone = parse(&["--tls-trust", "roots.pem"]).ok().unwrap().tls();
        assert!(alone.unwrap_err().contains("need --tls-cert"));
        let mutual = parse(&[
            "--tls-cert",
            "c",
            "--tls-key",
            "k",
            "--tls-client-auth",
            "NEEDED",
        ]);
        assert_eq!(
            mutual.ok().unwrap().tls().unwrap_err(),
            "Trust material must be present when requesting mutual TLS"
        );
        let mutual = parse(&[
            "--tls-cert",
            "c",
            "--tls-key",
            "k",
            "--tls-trust",
            "t",
            "--tls-client-auth",
            "WANTED",
        ]);
        let config = mutual.ok().unwrap().tls().unwrap().unwrap();
        assert_eq!(
            (config.trust, config.client_auth),
            (Some("t".into()), ClientAuth::Wanted)
        );
        let bad = parse(&["--tls-client-auth", "maybe"]).err().unwrap();
        assert!(
            bad.starts_with("--tls-client-auth: No enum constant"),
            "{bad}"
        );
    }

    /// The count reaches the engine: a table operation asks `parallel::threads`.
    #[test]
    fn the_thread_option_sets_the_count_table_operations_use() {
        let default = parallel::threads();
        let other = if default == 3 { 5 } else { 3 };
        let asked = parse(&["--threads", &other.to_string()]).ok().unwrap();
        assert_eq!(use_threads(&asked), other);
        assert_eq!(parallel::threads(), other);
        // On another thread too: the count is the process's.
        let elsewhere = std::thread::spawn(parallel::threads).join().unwrap();
        assert_eq!(elsewhere, other);
        // Without the option the count is left as it is.
        assert_eq!(use_threads(&parse(&[]).ok().unwrap()), other);
        parallel::set_threads(0);
        assert_eq!(parallel::threads(), default);
    }
}
