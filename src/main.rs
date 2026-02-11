use sppd_cli::cli;
use sppd_cli::errors::AppResult;
use tracing::info_span;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> AppResult<()> {
    // Initialize tracing subscriber with environment filter
    // Default to INFO level, but can be overridden with RUST_LOG env var
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let _span = info_span!("main").entered();

    cli::cli().await?;
    Ok(())
}
