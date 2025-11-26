use clap::{Command, Arg, ArgAction};
use std::collections::HashMap;
use crate::models::ProcurementType;
use crate::constants::{APP_VERSION, APP_AUTHOR, APP_ABOUT, PERIOD_HELP_TEXT};

pub fn cli(
    minor_contracts_links: HashMap<String, String>,
    public_tenders_links: HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let start_help = format!("Start {}", PERIOD_HELP_TEXT);
    let end_help = format!("End {}", PERIOD_HELP_TEXT);

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
            ProcurementType::MinorContracts => &minor_contracts_links,
            ProcurementType::PublicTenders => &public_tenders_links,
        };

        let start_period = matches.get_one::<String>("start").map(|s| s.as_str());
        let end_period = matches.get_one::<String>("end").map(|s| s.as_str());

        let filtered_links =
            crate::downloader::filter_periods_by_range(links, start_period, end_period)?;

        print_download_info(&proc_type, start_period, end_period);

        for (period, url) in filtered_links {
            println!("Period: {}, URL: {}", period, url);
        }
    }

    Ok(())
}

fn print_download_info(
    proc_type: &ProcurementType,
    start_period: Option<&str>,
    end_period: Option<&str>,
) {
    println!("\n📥 Downloading: {}", proc_type.display_name());
    if let Some(start) = start_period {
        println!("   Start period: {}", start);
    }
    if let Some(end) = end_period {
        println!("   End period: {}", end);
    }
    println!();
}
