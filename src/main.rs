mod api;
mod config;
mod db;
mod export;
mod model;
mod pipeline;
mod protocols;
mod supervisor;

use api::{create_router, AppState};
use config::AppConfig;
use db::DatabaseService;
use pipeline::InboundErrorNotifier;
use supervisor::SubscriptionManager;
use std::net::SocketAddr;
use std::path::Path;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup logging & tracing: console + file rotation
    let logs_dir = Path::new("logs");
    if !logs_dir.exists() {
        let _ = std::fs::create_dir_all(logs_dir);
    }
    let file_appender = tracing_appender::rolling::daily("logs", "moqui-device-gateway.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,moqui_device_gateway=debug"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().with_target(true))
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking).with_ansi(false))
        .init();

    info!("Starting Moqui Device Gateway (Rust mr-moqui edition)...");

    // 2. Load configuration
    let config = AppConfig::load();

    // 3. Validate security settings
    if let Err(sec_err) = config.validate_security() {
        error!("Security validation error: {}", sec_err);
        std::process::exit(1);
    }

    // 4. Connect to Moqui transactional & telemetry databases
    let db = match DatabaseService::init(&config.db_url, &config.log_db_url).await {
        Ok(d) => d,
        Err(e) => {
            error!("Failed to initialize database connection pools: {}", e);
            std::process::exit(1);
        }
    };

    // 5. Initialize InboundErrorNotifier
    let notifier = InboundErrorNotifier::new(
        config.inbound_error_notification_enabled,
        config.inbound_error_notification_threshold_seconds,
        config.inbound_error_notification_uri.clone(),
    );

    // 6. Initialize SubscriptionManager
    let manager = SubscriptionManager::new(db, config.clone(), notifier);

    // 7. Run startup discovery & restore subscriptions from DB
    manager.on_start().await;

    // 8. Build Axum web server
    let app = create_router(AppState {
        manager: manager.clone(),
    });

    let addr: SocketAddr = format!("{}:{}", config.http_host, config.http_port).parse()?;
    info!("Moqui Device Gateway listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Moqui Device Gateway shutdown gracefully.");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C signal, shutting down...");
        },
        _ = terminate => {
            info!("Received SIGTERM signal, shutting down...");
        },
    }
}
