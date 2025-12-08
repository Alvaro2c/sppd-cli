use crate::constants::{APP_ABOUT, APP_AUTHOR, APP_VERSION, PERIOD_HELP_TEXT};
use crate::downloader::{download_files, filter_periods_by_range};
use crate::errors::AppResult;
use crate::models::ProcurementType;
use clap::{Arg, ArgAction, Command};
use std::collections::BTreeMap;
use tracing::{info, info_span};

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
                ),
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("download") {
        let proc_type = ProcurementType::from(
            matches
                .get_one::<String>("type")
                .map(|s| s.as_str())
                .unwrap_or("public-tenders"),
        );

        let links = match proc_type {
            ProcurementType::MinorContracts => minor_contracts_links,
            ProcurementType::PublicTenders => public_tenders_links,
        };

        let start_period = matches.get_one::<String>("start").map(|s| s.as_str());
        let end_period = matches.get_one::<String>("end").map(|s| s.as_str());

        let filtered_links = filter_periods_by_range(links, start_period, end_period)?;

        print_download_info(&proc_type, start_period, end_period);

        download_files(&filtered_links, &proc_type).await?;
    }

    Ok(())
}

fn print_download_info(
    proc_type: &ProcurementType,
    start_period: Option<&str>,
    end_period: Option<&str>,
) {
    let _span = info_span!("download", 
        procurement_type = proc_type.display_name(),
        start_period = start_period,
        end_period = end_period
    ).entered();
    
    info!(
        procurement_type = proc_type.display_name(),
        start_period = start_period,
        end_period = end_period,
        "📥 Starting download"
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
        );
        print_download_info(&ProcurementType::PublicTenders, None, None);
    }
}
