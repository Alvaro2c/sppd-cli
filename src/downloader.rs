use crate::constants::{MINOR_CONTRACTS, PERIOD_REGEX_PATTERN, PUBLIC_TENDERS, ZIP_LINK_SELECTOR};
use crate::errors::{AppError, AppResult};
use crate::models::ProcurementType;
use reqwest;
use scraper::{Html, Selector};
use std::collections::BTreeMap;
use std::path::Path;
use std::str::FromStr;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use url::Url;

/// Fetch all available zip links for both sources using a shared `reqwest::Client`.
pub async fn fetch_all_links() -> AppResult<(BTreeMap<String, String>, BTreeMap<String, String>)> {
    let client = reqwest::Client::new();
    // Sequential fetch: simple and reliable for two landing pages.
    let minor_links = fetch_zip(&client, MINOR_CONTRACTS).await?;
    let public_links = fetch_zip(&client, PUBLIC_TENDERS).await?;
    Ok((minor_links, public_links))
}

/// Fetch zip links from a single page asynchronously using the provided client.
pub async fn fetch_zip(
    client: &reqwest::Client,
    input_url: &str,
) -> AppResult<BTreeMap<String, String>> {
    // parse the base URL
    let base_url = Url::parse(input_url)?;

    // fetch the page content
    let response = client
        .get(base_url.as_str())
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let document = Html::parse_document(&response);

    // selector to find all links ending with .zip
    let selector = Selector::parse(ZIP_LINK_SELECTOR).map_err(|_| {
        AppError::SelectorError(format!("Failed to parse selector '{}'", ZIP_LINK_SELECTOR))
    })?;

    let mut links: BTreeMap<String, String> = BTreeMap::new();
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
    links: &BTreeMap<String, String>,
    start_period: Option<&str>,
    end_period: Option<&str>,
) -> AppResult<BTreeMap<String, String>> {
    let mut filtered = BTreeMap::new();

    let start_period_num = start_period.and_then(|s| u32::from_str(s).ok());
    let end_period_num = end_period.and_then(|s| u32::from_str(s).ok());

    // Get sorted list of available periods as owned Strings (deterministic order)
    // BTreeMap keys are already ordered deterministically
    let available_periods: Vec<String> = links.keys().cloned().collect();
    let available_str = available_periods.join(", ");

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

    for (period, url) in links.iter() {
        if let Ok(period_num) = u32::from_str(period) {
            let in_range = match (start_period_num, end_period_num) {
                (Some(start), Some(end)) => period_num >= start && period_num <= end,
                (Some(start), None) => period_num >= start,
                (None, Some(end)) => period_num <= end,
                (None, None) => true,
            };

            if in_range {
                filtered.insert(period.to_owned(), url.to_owned());
            }
        }
    }

    Ok(filtered)
}

pub async fn download_files(
    filtered_links: &BTreeMap<String, String>,
    proc_type: &ProcurementType,
) -> AppResult<()> {
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
        // Skip download if final file already exists
        if file_path.exists() {
            println!("Skipping existing: {}", file_path.display());
            continue;
        }

        // Temporary partial file (atomic rename after complete)
        let tmp_path = download_dir.join(format!("{}.zip.part", period));

        // Remove stale tmp file if present (best-effort)
        if tmp_path.exists() {
            if let Err(e) = fs::remove_file(&tmp_path).await {
                eprintln!(
                    "Warning: failed to remove stale temp file {}: {}",
                    tmp_path.display(),
                    e
                );
            }
        }

        println!("Downloading: {} -> {}", url, file_path.display());

        let mut response = client.get(url).send().await?.error_for_status()?;

        let mut file = File::create(&tmp_path).await.map_err(|e| {
            AppError::IoError(format!(
                "Failed to create temp file {}: {}",
                tmp_path.display(),
                e
            ))
        })?;

        while let Some(chunk) = response.chunk().await? {
            file.write_all(&chunk).await.map_err(|e| {
                AppError::IoError(format!(
                    "Failed to write to temp file {}: {}",
                    tmp_path.display(),
                    e
                ))
            })?;
        }

        // Ensure the file is closed before renaming
        drop(file);

        // Atomically move the temp file to the final destination
        fs::rename(&tmp_path, &file_path).await.map_err(|e| {
            AppError::IoError(format!(
                "Failed to rename temp file {} to {}: {}",
                tmp_path.display(),
                file_path.display(),
                e
            ))
        })?;

        println!("✓ Downloaded: {}", filename);
    }

    Ok(())
}
