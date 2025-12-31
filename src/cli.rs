use crate::constants::{APP_ABOUT, APP_AUTHOR, APP_VERSION, PERIOD_HELP_TEXT};
use crate::downloader::{download_files, filter_periods_by_range};
use crate::errors::AppResult;
use crate::extractor::extract_all_zips;
use crate::models::ProcurementType;
use crate::parser::{cleanup_files, parse_xmls};
use clap::{Arg, ArgAction, Command};
use std::collections::BTreeMap;
use tracing::{info, info_span};

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
/// # Example
///
/// Typically called from the main binary after fetching links:
///
/// ```no_run
/// use sppd_cli::{cli, downloader, errors::AppResult};
///
/// # async fn example() -> AppResult<()> {
/// let (minor_links, public_links) = downloader::fetch_all_links().await?;
/// cli::cli(&minor_links, &public_links).await?;
/// # Ok(())
/// # }
/// ```
pub async fn cli(
    minor_contracts_links: &BTreeMap<String, String>,
    public_tenders_links: &BTreeMap<String, String>,
) -> AppResult<()> {
    let start_help = format!("Start {PERIOD_HELP_TEXT}");
    let end_help = format!("End {PERIOD_HELP_TEXT}");

    let matches = Command::new("sppd-cli")
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
                ),
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("download") {
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
        let should_cleanup = parse_yes_no(cleanup_value);

        let target_links = filter_periods_by_range(links, start_period, end_period)?;

        print_download_info(&proc_type, start_period, end_period, target_links.len());

        let client = reqwest::Client::new();
        download_files(&client, &target_links, &proc_type).await?;

        info!("Starting extraction phase");
        extract_all_zips(&target_links, &proc_type).await?;

        info!("Starting parsing phase");
        parse_xmls(&target_links, &proc_type)?;

        cleanup_files(&target_links, &proc_type, should_cleanup).await?;

        info!(
            procurement_type = proc_type.display_name(),
            periods_processed = target_links.len(),
            "All operations completed successfully"
        );
    }

    Ok(())
}

/// Parses a yes/no string value (case-insensitive) and returns a boolean.
/// Accepts "yes", "y", "no", "n". Defaults to true for any unrecognized value.
pub(crate) fn parse_yes_no(value: &str) -> bool {
    match value.trim().to_lowercase().as_str() {
        "yes" | "y" => true,
        "no" | "n" => false,
        _ => true, // Default to true for any unrecognized value
    }
}

fn print_download_info(
    proc_type: &ProcurementType,
    start_period: Option<&str>,
    end_period: Option<&str>,
    periods_count: usize,
) {
    let _span = info_span!(
        "download",
        procurement_type = proc_type.display_name(),
        start_period = start_period,
        end_period = end_period
    )
    .entered();

    info!(
        procurement_type = proc_type.display_name(),
        start_period = start_period,
        end_period = end_period,
        periods_count = periods_count,
        "Starting download operation"
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
        assert!(parse_yes_no("yes"));
        assert!(parse_yes_no("YES"));
        assert!(parse_yes_no("Yes"));
        assert!(parse_yes_no("y"));
        assert!(parse_yes_no("Y"));
    }

    #[test]
    fn test_parse_yes_no_no() {
        assert!(!parse_yes_no("no"));
        assert!(!parse_yes_no("NO"));
        assert!(!parse_yes_no("No"));
        assert!(!parse_yes_no("n"));
        assert!(!parse_yes_no("N"));
    }

    #[test]
    fn test_parse_yes_no_whitespace() {
        assert!(parse_yes_no(" yes "));
        assert!(parse_yes_no("  YES  "));
        assert!(!parse_yes_no(" no "));
        assert!(!parse_yes_no("  NO  "));
        assert!(parse_yes_no("\ty\t"));
        assert!(!parse_yes_no("\tn\t"));
    }

    #[test]
    fn test_parse_yes_no_defaults_to_true() {
        assert!(parse_yes_no(""));
        assert!(parse_yes_no("unknown"));
        assert!(parse_yes_no("maybe"));
        assert!(parse_yes_no("1"));
        assert!(parse_yes_no("0"));
        assert!(parse_yes_no("true"));
        assert!(parse_yes_no("false"));
    }
}
