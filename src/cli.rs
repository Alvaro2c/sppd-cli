use crate::config::{ResolvedConfig, ResolvedConfigFile};
use crate::downloader::{download_files, filter_periods_by_range};
use crate::errors::{AppError, AppResult};
use crate::extractor::extract_all_zips;
use crate::models::ProcurementType;
use crate::parser::{cleanup_files, parse_xmls};
use clap::{Arg, ArgAction, Command};
use std::collections::BTreeMap;
use std::path::PathBuf;
use tracing::info;

// CLI metadata constants
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const APP_AUTHOR: &str = env!("CARGO_PKG_AUTHORS");
const APP_ABOUT: &str = env!("CARGO_PKG_DESCRIPTION");
/// Parses command-line arguments and executes the download command.
///
/// This function handles two subcommands:
/// - `cli`: Manual CLI with default configuration (cleanup always enabled)
/// - `toml`: Run using a TOML configuration file (cleanup configurable)
///
/// Both subcommands execute the same workflow for downloading and processing procurement data:
/// 1. Parses CLI arguments (procurement type, period range, cleanup options)
/// 2. Filters available links by the specified period range
/// 3. Downloads ZIP files from the filtered URLs
/// 4. Extracts ZIP archives to access XML/Atom files
/// 5. Parses XML/Atom content and converts to Parquet format
/// 6. Performs cleanup if requested
///
/// # Arguments
///
/// * `minor_contracts_links` - Map of period strings (e.g., "202301") to minor contracts download URLs
/// * `public_tenders_links` - Map of period strings (e.g., "202301") to public tenders download URLs
///
/// # Returns
///
/// Returns `Ok(())` if all operations complete successfully. Returns an error if:
/// - Invalid period ranges are specified
/// - Network requests fail
/// - File I/O operations fail
/// - XML parsing fails
///
pub async fn cli(
    minor_contracts_links: &BTreeMap<String, String>,
    public_tenders_links: &BTreeMap<String, String>,
) -> AppResult<()> {
    let cmd = Command::new("sppd-cli")
        .version(APP_VERSION)
        .author(APP_AUTHOR)
        .about(APP_ABOUT)
        .subcommand(
            Command::new("cli")
                .about("Download, extract, parse, and clean a period range")
                .after_help("Uses batch_size=150, concat disabled by default.\nExample:\n  sppd-cli cli -t public-tenders -s 2023 -e 2023 --concat-batches")
                .arg(
                    Arg::new("type")
                        .short('t')
                        .long("type")
                        .help("Procurement type: 'minor-contracts' (mc, min) or 'public-tenders' (pt, pub)")
                        .default_value("public-tenders")
                        .action(ArgAction::Set),
                )
                .arg(
                    Arg::new("start")
                        .short('s')
                        .long("start")
                        .help("First period to download and parse (YYYY or YYYYMM)")
                        .action(ArgAction::Set),
                )
                .arg(
                    Arg::new("end")
                        .short('e')
                        .long("end")
                        .help("Last period to download and parse (YYYY or YYYYMM)")
                        .action(ArgAction::Set),
                )
                .arg(
                    Arg::new("read_concurrency")
                        .short('r')
                        .long("read-concurrency")
                        .alias("rc")
                        .help("Files read in parallel while parsing XML")
                        .value_parser(clap::value_parser!(usize))
                        .action(ArgAction::Set),
                )
                .arg(
                    Arg::new("concat_batches")
                        .short('c')
                        .long("concat-batches")
                        .alias("cb")
                        .help("Merge the per-batch parquet files after parsing")
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("toml")
                .about("Run using a TOML configuration file")
                .arg(
                    Arg::new("config")
                        .help("Path to the TOML config file")
                        .required(true)
                        .value_parser(clap::value_parser!(PathBuf)),
                ),
        );

    let mut cmd_for_help = cmd.clone();
    let matches = cmd.get_matches();

    match matches.subcommand() {
        Some(("cli", sub)) => {
            let proc_type = ProcurementType::from(
                sub.get_one::<String>("type")
                    .expect("type has default_value")
                    .as_str(),
            );
            let start_period = sub.get_one::<String>("start").map(|s| s.as_str());
            let end_period = sub.get_one::<String>("end").map(|s| s.as_str());
            let mut resolved_config = ResolvedConfig::default();
            if let Some(&concurrency) = sub.get_one::<usize>("read_concurrency") {
                resolved_config.read_concurrency = concurrency;
            }
            if sub.get_flag("concat_batches") {
                resolved_config.concat_batches = true;
            }

            run_workflow(
                minor_contracts_links,
                public_tenders_links,
                proc_type,
                start_period,
                end_period,
                true,
                &resolved_config,
            )
            .await?;
        }
        Some(("toml", sub)) => {
            let config_path = sub
                .get_one::<PathBuf>("config")
                .expect("config is required");

            let file_config = ResolvedConfigFile::from_toml_file(config_path)?;
            let proc_type = ProcurementType::from(file_config.procurement_type.as_str());
            let start_period = Some(file_config.start.as_str());
            let end_period = Some(file_config.end.as_str());

            run_workflow(
                minor_contracts_links,
                public_tenders_links,
                proc_type,
                start_period,
                end_period,
                file_config.cleanup,
                &file_config.resolved,
            )
            .await?;
        }
        _ => {
            cmd_for_help
                .print_help()
                .map_err(|e| AppError::IoError(format!("Failed to print help: {e}")))?;
        }
    }

    Ok(())
}

async fn run_workflow(
    minor_contracts_links: &BTreeMap<String, String>,
    public_tenders_links: &BTreeMap<String, String>,
    proc_type: ProcurementType,
    start_period: Option<&str>,
    end_period: Option<&str>,
    should_cleanup: bool,
    resolved_config: &ResolvedConfig,
) -> AppResult<()> {
    let links = match proc_type {
        ProcurementType::MinorContracts => minor_contracts_links,
        ProcurementType::PublicTenders => public_tenders_links,
    };

    let target_links = filter_periods_by_range(links, start_period, end_period)?;

    print_download_info(&proc_type, start_period, end_period, target_links.len());

    let client = reqwest::Client::new();
    download_files(&client, &target_links, &proc_type, resolved_config).await?;

    info!("Starting extraction phase");
    extract_all_zips(&target_links, &proc_type, resolved_config).await?;

    parse_xmls(
        &target_links,
        &proc_type,
        resolved_config.batch_size,
        resolved_config,
    )
    .await?;

    cleanup_files(&target_links, &proc_type, should_cleanup, resolved_config).await?;

    info!(
        procurement_type = proc_type.display_name(),
        periods_processed = target_links.len(),
        "All operations completed successfully"
    );

    Ok(())
}

fn print_download_info(
    proc_type: &ProcurementType,
    start_period: Option<&str>,
    end_period: Option<&str>,
    periods_count: usize,
) {
    let start_text = start_period.unwrap_or("first available");
    let end_text = end_period.unwrap_or("last available");
    info!(
        procurement_type = proc_type.display_name(),
        periods = periods_count,
        start_period = start_text,
        end_period = end_text,
        "Starting download"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Command;

    #[test]
    fn cli_command_parses_defaults() {
        let cmd = Command::new("sppd-cli").subcommand(
            Command::new("cli").arg(
                clap::Arg::new("type")
                    .short('t')
                    .long("type")
                    .default_value("public-tenders"),
            ),
        );

        let matches = cmd.try_get_matches_from(vec!["sppd-cli", "cli"]).unwrap();
        let sub = matches.subcommand_matches("cli").unwrap();
        let t = sub
            .get_one::<String>("type")
            .map(|s| s.as_str())
            .unwrap_or("public-tenders");
        let proc_type = ProcurementType::from(t);
        assert!(matches!(proc_type, ProcurementType::PublicTenders));
    }

    #[test]
    fn toml_command_requires_path() {
        let cmd = Command::new("sppd-cli")
            .subcommand(Command::new("toml").arg(clap::Arg::new("config").required(true)));
        let err = cmd.try_get_matches_from(vec!["sppd-cli", "toml"]);
        assert!(err.is_err());
    }

    #[test]
    fn test_print_download_info_runs() {
        print_download_info(
            &ProcurementType::MinorContracts,
            Some("202301"),
            Some("202302"),
            3,
        );
        print_download_info(&ProcurementType::PublicTenders, None, None, 5);
    }
}
