use sppd_cli::{cli, downloader, errors::AppResult};

#[tokio::main]
async fn main() -> AppResult<()> {
    let (minor_contracts_links, public_tenders_links) = downloader::fetch_all_links().await?;
    cli::cli(&minor_contracts_links, &public_tenders_links).await?;
    Ok(())
}
