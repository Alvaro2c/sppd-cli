use sppd_cli::{cli, downloader, errors};
use errors::AppResult;

fn main() -> AppResult<()> {
    let (minor_contracts_links, public_tenders_links) = downloader::fetch_all_links()?;
    cli::cli(minor_contracts_links, public_tenders_links)?;
    Ok(())
}
