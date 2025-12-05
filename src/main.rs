use sppd_cli::{cli, downloader, errors};
use errors::AppResult;

fn main() -> AppResult<()> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| errors::AppError::IoError(e.to_string()))?;

    let (minor_contracts_links, public_tenders_links) = rt.block_on(downloader::fetch_all_links())?;
    cli::cli(&minor_contracts_links, &public_tenders_links)?;
    Ok(())
}
