use crate::config::ResolvedConfig;
use crate::downloader::{download_files, filter_periods_by_range};
use crate::errors::{AppError, AppResult};
use crate::extractor::extract_all_zips;
use crate::models::ProcurementType;
use crate::parser::{cleanup_files, parse_xmls};
use clap::{Arg, ArgAction, Command};
use std::collections::BTreeMap;
use tracing::info;

// CLI metadata constants
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const APP_AUTHOR: &str = env!("CARGO_PKG_AUTHORS");
const APP_ABOUT: &str = env!("CARGO_PKG_DESCRIPTION");
const PERIOD_HELP_TEXT: &str = "Period (YYYY or YYYYMM format, e.g., 202301)";

/// Parses command-line arguments and executes the download command.
///
/// This function handles the complete workflow for downloading and processing procurement data:
/// 1. Parses CLI arguments (procurement type, period range, cleanup options)
/// 2. Filters available links by the specified period range
/// 3. Downloads ZIP files from the filtered URLs
/// 4. Extracts ZIP archives to access XML/Atom files
/// 5. Parses XML/Atom content and converts to Parquet format
/// 6. Optionally cleans up temporary files (ZIP and extracted directories)
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
    let start_help = format!("Start {PERIOD_HELP_TEXT}");
    let end_help = format!("End {PERIOD_HELP_TEXT}");

    let cmd = Command::new("sppd-cli")
        .version(APP_VERSION)
        .author(APP_AUTHOR)
        .about(APP_ABOUT)
        .subcommand(
            Command::new("download")
                .about("Download procurement data")
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
                        .help(start_help.as_str())
                        .action(ArgAction::Set),
                )
                .arg(
                    Arg::new("end")
                        .short('e')
                        .long("end")
                        .help(end_help.as_str())
                        .action(ArgAction::Set),
                )
                .arg(
                    Arg::new("cleanup")
                        .long("cleanup")
                        .help("Delete ZIP and XML/Atom files after processing, keeping only Parquet files (yes/no)")
                        .default_value("yes")
                        .action(ArgAction::Set),
                )
                .arg(
                    Arg::new("batch-size")
                        .long("batch-size")
                        .help("Number of XML files to process per batch (default: 100, can also be set via SPPD_BATCH_SIZE env var)")
                        .value_parser(clap::value_parser!(usize))
                        .action(ArgAction::Set),
                ),
        );

    let mut cmd_for_help = cmd.clone();
    let matches = cmd.get_matches();

    if let Some(matches) = matches.subcommand_matches("download") {
        // Resolve configuration with precedence: CLI > env vars > defaults
        let cli_batch_size = matches.get_one::<usize>("batch-size").copied();
        let resolved_config = ResolvedConfig::from_cli_and_env(cli_batch_size);

        // Validate batch size
        if resolved_config.batch_size == 0 {
            return Err(AppError::InvalidInput(
                "Batch size must be greater than 0".to_string(),
            ));
        }

        let proc_type = ProcurementType::from(
            matches
                .get_one::<String>("type")
                .expect("type has default_value")
                .as_str(),
        );

        let links = match proc_type {
            ProcurementType::MinorContracts => minor_contracts_links,
            ProcurementType::PublicTenders => public_tenders_links,
        };

        let start_period = matches.get_one::<String>("start").map(|s| s.as_str());
        let end_period = matches.get_one::<String>("end").map(|s| s.as_str());

        let cleanup_value = matches
            .get_one::<String>("cleanup")
            .expect("cleanup has default_value")
            .as_str();
        let should_cleanup = parse_yes_no(cleanup_value)?;

        let target_links = filter_periods_by_range(links, start_period, end_period)?;

        print_download_info(&proc_type, start_period, end_period, target_links.len());

        let client = reqwest::Client::new();
        download_files(&client, &target_links, &proc_type, &resolved_config).await?;

        info!("Starting extraction phase");
        extract_all_zips(&target_links, &proc_type, &resolved_config).await?;

        parse_xmls(
            &target_links,
            &proc_type,
            resolved_config.batch_size,
            &resolved_config,
        )?;

        cleanup_files(&target_links, &proc_type, should_cleanup, &resolved_config).await?;

        info!(
            procurement_type = proc_type.display_name(),
            periods_processed = target_links.len(),
            "All operations completed successfully"
        );
    } else {
        // No subcommand provided, show help
        cmd_for_help
            .print_help()
            .map_err(|e| AppError::IoError(format!("Failed to print help: {e}")))?;
    }

    Ok(())
}

/// Parses a yes/no string value (case-insensitive) and returns a boolean.
/// Accepts "yes", "y", "no", "n". Returns an error for unrecognized values.
fn parse_yes_no(value: &str) -> AppResult<bool> {
    match value.trim().to_lowercase().as_str() {
        "yes" | "y" => Ok(true),
        "no" | "n" => Ok(false),
        _ => Err(AppError::InvalidInput(format!(
            "Invalid cleanup value: {value}. Expected 'yes', 'y', 'no', or 'n'"
        ))),
    }
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
    fn test_command_parsing_defaults() {
        let cmd = Command::new("sppd-cli").subcommand(
            Command::new("download").arg(
                clap::Arg::new("type")
                    .short('t')
                    .long("type")
                    .default_value("public-tenders"),
            ),
        );

        let matches = cmd
            .try_get_matches_from(vec!["sppd-cli", "download"])
            .unwrap();
        let sub = matches.subcommand_matches("download").unwrap();
        let t = sub
            .get_one::<String>("type")
            .map(|s| s.as_str())
            .unwrap_or("public-tenders");
        let proc_type = ProcurementType::from(t);
        assert!(matches!(proc_type, ProcurementType::PublicTenders));
    }

    #[test]
    fn test_type_aliases_mapping() {
        let cases = vec![
            ("mc", ProcurementType::MinorContracts),
            ("min", ProcurementType::MinorContracts),
            ("pt", ProcurementType::PublicTenders),
            ("pub", ProcurementType::PublicTenders),
        ];

        for (alias, expected) in cases {
            let cmd = Command::new("sppd-cli").subcommand(
                Command::new("download").arg(
                    clap::Arg::new("type")
                        .short('t')
                        .long("type")
                        .default_value("public-tenders"),
                ),
            );
            let matches = cmd
                .try_get_matches_from(vec!["sppd-cli", "download", "-t", alias])
                .unwrap();
            let sub = matches.subcommand_matches("download").unwrap();
            let t = sub
                .get_one::<String>("type")
                .map(|s| s.as_str())
                .unwrap_or("public-tenders");
            let proc_type = ProcurementType::from(t);
            match expected {
                ProcurementType::MinorContracts => {
                    assert!(matches!(proc_type, ProcurementType::MinorContracts))
                }
                ProcurementType::PublicTenders => {
                    assert!(matches!(proc_type, ProcurementType::PublicTenders))
                }
            }
        }
    }

    #[test]
    fn test_start_end_extraction() {
        let cmd = Command::new("sppd-cli").subcommand(
            Command::new("download")
                .arg(
                    clap::Arg::new("start")
                        .short('s')
                        .long("start")
                        .action(ArgAction::Set),
                )
                .arg(
                    clap::Arg::new("end")
                        .short('e')
                        .long("end")
                        .action(ArgAction::Set),
                ),
        );

        let matches = cmd
            .try_get_matches_from(vec!["sppd-cli", "download", "-s", "202301", "-e", "202302"])
            .unwrap();
        let sub = matches.subcommand_matches("download").unwrap();
        let start = sub.get_one::<String>("start").map(|s| s.as_str());
        let end = sub.get_one::<String>("end").map(|s| s.as_str());
        assert_eq!(start, Some("202301"));
        assert_eq!(end, Some("202302"));
    }

    #[test]
    fn test_print_download_info_runs() {
        // Ensure the function runs without panic for various inputs
        print_download_info(
            &ProcurementType::MinorContracts,
            Some("202301"),
            Some("202302"),
            3,
        );
        print_download_info(&ProcurementType::PublicTenders, None, None, 5);
    }

    #[test]
    fn test_parse_yes_no_yes() {
        assert!(parse_yes_no("yes").unwrap());
        assert!(parse_yes_no("YES").unwrap());
        assert!(parse_yes_no("Yes").unwrap());
        assert!(parse_yes_no("y").unwrap());
        assert!(parse_yes_no("Y").unwrap());
    }

    #[test]
    fn test_parse_yes_no_no() {
        assert!(!parse_yes_no("no").unwrap());
        assert!(!parse_yes_no("NO").unwrap());
        assert!(!parse_yes_no("No").unwrap());
        assert!(!parse_yes_no("n").unwrap());
        assert!(!parse_yes_no("N").unwrap());
    }

    #[test]
    fn test_parse_yes_no_whitespace() {
        assert!(parse_yes_no(" yes ").unwrap());
        assert!(parse_yes_no("  YES  ").unwrap());
        assert!(!parse_yes_no(" no ").unwrap());
        assert!(!parse_yes_no("  NO  ").unwrap());
        assert!(parse_yes_no("\ty\t").unwrap());
        assert!(!parse_yes_no("\tn\t").unwrap());
    }

    #[test]
    fn test_parse_yes_no_invalid_values_return_error() {
        let invalid_values = vec!["", "unknown", "maybe", "1", "0", "true", "false", "maybe"];

        for value in invalid_values {
            let result = parse_yes_no(value);
            assert!(result.is_err(), "Expected error for value: {value}");
            match result.unwrap_err() {
                AppError::InvalidInput(msg) => {
                    assert!(msg.contains("Invalid cleanup value"));
                    assert!(msg.contains(value));
                }
                _ => panic!("Expected InvalidInput error for value: {value}"),
            }
        }
    }
}
