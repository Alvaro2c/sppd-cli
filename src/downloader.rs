use crate::constants::{MINOR_CONTRACTS, PUBLIC_TENDERS, ZIP_LINK_SELECTOR, PERIOD_REGEX_PATTERN};
use crate::models::ProcurementType;
use crate::errors::{AppError, AppResult};
use reqwest;
use scraper::{Html, Selector};
use std::collections::HashMap;
use std::str::FromStr;
use std::path::Path;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use url::Url;

pub fn fetch_all_links() -> AppResult<(HashMap<String, String>, HashMap<String, String>)> {
    let minor_links = fetch_zip(MINOR_CONTRACTS)?;
    let public_links = fetch_zip(PUBLIC_TENDERS)?;
    Ok((minor_links, public_links))
}


pub fn fetch_zip(input_url: &str) -> AppResult<HashMap<String, String>> {
    // parse the base URL
    let base_url = Url::parse(input_url)?;

    // fetch the page content
    let response = reqwest::blocking::get(base_url.as_str())?
        .error_for_status()?
        .text()?;
    let document = Html::parse_document(&response);

    // selector to find all links ending with .zip
    let selector = Selector::parse(ZIP_LINK_SELECTOR)
        .map_err(|_| AppError::SelectorError(format!("Failed to parse selector '{}'", ZIP_LINK_SELECTOR)))?;

    let mut links: HashMap<String, String> = HashMap::new();
    let re = regex::Regex::new(PERIOD_REGEX_PATTERN)?;

    for url in document
        .select(&selector)
        .filter_map(|el| el.value().attr("href"))
        .filter_map(|href| base_url.join(href).ok())
    {
        if let Some(filename) = url.path_segments().and_then(|s| s.last()) {
            if let Some(m) = re.captures(filename).and_then(|c| c.get(1)) {
                links.insert(m.as_str().to_string(), url.to_string());
            }
        }
    }

    Ok(links)
}

pub fn filter_periods_by_range(
    links: &HashMap<String, String>,
    start_period: Option<&str>,
    end_period: Option<&str>,
) -> AppResult<HashMap<String, String>> {
    let mut filtered = HashMap::new();

    let start_period_num = start_period.and_then(|s| u32::from_str(s).ok());
    let end_period_num = end_period.and_then(|s| u32::from_str(s).ok());

    // Get sorted list of available periods
    let mut available_periods: Vec<_> = links.keys().collect();
    available_periods.sort();
    let available_str = available_periods
        .iter()
        .map(|p| p.as_str())
        .collect::<Vec<_>>()
        .join(", ");

    // Validate that specified periods exist in links
    let validate_period = |period: Option<&str>| -> AppResult<()> {
        if let Some(p) = period {
            if !links.contains_key(p) {
                return Err(AppError::PeriodValidationError {
                    period: p.to_string(),
                    available: available_str.clone(),
                });
            }
        }
        Ok(())
    };

    validate_period(start_period)?;
    validate_period(end_period)?;

    for (period, url) in links {
        if let Ok(period_num) = u32::from_str(period) {
            let in_range = match (start_period_num, end_period_num) {
                (Some(start), Some(end)) => period_num >= start && period_num <= end,
                (Some(start), None) => period_num >= start,
                (None, Some(end)) => period_num <= end,
                (None, None) => true,
            };

            if in_range {
                filtered.insert(period.clone(), url.clone());
            }
        }
    }

    Ok(filtered)
}

pub async fn download_files(filtered_links: &HashMap<String, String>, proc_type: &ProcurementType) -> AppResult<()> {
    let download_dir = match proc_type {
        ProcurementType::MinorContracts => Path::new("data/tmp/mc"),
        ProcurementType::PublicTenders => Path::new("data/tmp/pt"),
    };

    // Create directory if it doesn't exist
    if !download_dir.exists() {
        fs::create_dir_all(download_dir)
            .await
            .map_err(|e| AppError::IoError(format!("Failed to create directory: {}", e)))?;
    }

    let client = reqwest::Client::new();

    for (period, url) in filtered_links {
        let filename = format!("{}.zip", period);
        let file_path = download_dir.join(&filename);

        println!("Downloading: {} -> {}", url, file_path.display());

        let mut response = client.get(url).send().await?.error_for_status()?;

        let mut file = File::create(&file_path)
            .await
            .map_err(|e| AppError::IoError(format!("Failed to create file {}: {}", file_path.display(), e)))?;

        while let Some(chunk) = response.chunk().await? {
            file.write_all(&chunk).await.map_err(|e| {
                AppError::IoError(format!("Failed to write to file {}: {}", file_path.display(), e))
            })?;
        }

        println!("✓ Downloaded: {}", filename);
    }

    Ok(())
}
