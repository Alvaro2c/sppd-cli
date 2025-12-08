use sppd_cli::{cli, downloader, errors::AppResult};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> AppResult<()> {
    // Initialize tracing subscriber with environment filter
    // Default to INFO level, but can be overridden with RUST_LOG env var
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let (minor_contracts_links, public_tenders_links) = downloader::fetch_all_links().await?;
    cli::cli(&minor_contracts_links, &public_tenders_links).await?;
    Ok(())
}
