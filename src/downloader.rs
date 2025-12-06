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

    parse_zip_links(&response, &base_url)
}

/// Parse HTML response and extract `.zip` links keyed by detected period string.
pub fn parse_zip_links(html: &str, base_url: &Url) -> AppResult<BTreeMap<String, String>> {
    let document = Html::parse_document(html);

    let selector = Selector::parse(ZIP_LINK_SELECTOR).map_err(|_| {
        AppError::SelectorError(format!("Failed to parse selector '{ZIP_LINK_SELECTOR}'"))
    })?;

    let mut links: BTreeMap<String, String> = BTreeMap::new();
    let re = regex::Regex::new(PERIOD_REGEX_PATTERN)?;

    for url in document
        .select(&selector)
        .filter_map(|el| el.value().attr("href"))
        .filter_map(|href| base_url.join(href).ok())
    {
        if let Some(filename) = url.path_segments().and_then(|mut s| s.next_back()) {
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
            .map_err(|e| AppError::IoError(format!("Failed to create directory: {e}")))?;
    }

    let client = reqwest::Client::new();

    for (period, url) in filtered_links {
        let filename = format!("{period}.zip");
        let file_path = download_dir.join(&filename);
        // Skip download if final file already exists
        if file_path.exists() {
            println!("Skipping existing: {}", file_path.display());
            continue;
        }

        // Temporary partial file (atomic rename after complete)
        let tmp_path = download_dir.join(format!("{period}.zip.part"));

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

        println!("✓ Downloaded: {filename}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{filter_periods_by_range, parse_zip_links};
    use crate::errors::AppError;
    use url::Url;
    use std::collections::BTreeMap;

    fn create_test_links() -> BTreeMap<String, String> {
        let mut links = BTreeMap::new();
        links.insert(
            "202301".to_string(),
            "https://example.com/202301.zip".to_string(),
        );
        links.insert(
            "202302".to_string(),
            "https://example.com/202302.zip".to_string(),
        );
        links.insert(
            "202303".to_string(),
            "https://example.com/202303.zip".to_string(),
        );
        links.insert(
            "202304".to_string(),
            "https://example.com/202304.zip".to_string(),
        );
        links.insert(
            "202305".to_string(),
            "https://example.com/202305.zip".to_string(),
        );
        links
    }

    #[test]
    fn test_filter_all_periods_no_constraints() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, None, None);

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 5);
    }

    #[test]
    fn test_filter_with_start_period_only() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, Some("202303"), None);

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 3); // 202303, 202304, 202305
        assert!(filtered.contains_key("202303"));
        assert!(filtered.contains_key("202305"));
        assert!(!filtered.contains_key("202302"));
    }

    #[test]
    fn test_filter_with_end_period_only() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, None, Some("202303"));

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 3); // 202301, 202302, 202303
        assert!(filtered.contains_key("202301"));
        assert!(filtered.contains_key("202303"));
        assert!(!filtered.contains_key("202304"));
    }

    #[test]
    fn test_filter_with_start_and_end_period() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, Some("202302"), Some("202304"));

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 3); // 202302, 202303, 202304
        assert!(filtered.contains_key("202302"));
        assert!(filtered.contains_key("202303"));
        assert!(filtered.contains_key("202304"));
        assert!(!filtered.contains_key("202301"));
        assert!(!filtered.contains_key("202305"));
    }

    #[test]
    fn test_filter_single_period() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, Some("202303"), Some("202303"));

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 1);
        assert!(filtered.contains_key("202303"));
    }

    #[test]
    fn test_filter_invalid_start_period() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, Some("999999"), None);

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::PeriodValidationError { period, .. } => {
                assert_eq!(period, "999999");
            }
            _ => panic!("Expected PeriodValidationError"),
        }
    }

    #[test]
    fn test_filter_invalid_end_period() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, None, Some("999999"));

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::PeriodValidationError { period, .. } => {
                assert_eq!(period, "999999");
            }
            _ => panic!("Expected PeriodValidationError"),
        }
    }

    #[test]
    fn test_filter_both_periods_invalid() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, Some("999999"), Some("888888"));

        // Should fail on the first invalid period (start)
        assert!(result.is_err());
    }

    #[test]
    fn test_filter_error_includes_available_periods() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, Some("999999"), None);

        assert!(result.is_err());
        if let AppError::PeriodValidationError { available, .. } = result.unwrap_err() {
            // Available periods should be comma-separated and sorted
            assert!(available.contains("202301"));
            assert!(available.contains("202305"));
        } else {
            panic!("Expected PeriodValidationError");
        }
    }

    #[test]
    fn test_filter_empty_hash_map() {
        let links = BTreeMap::new();
        let result = filter_periods_by_range(&links, None, None);

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_filter_preserves_urls() {
        let mut links = BTreeMap::new();
        let url1 = "https://example.com/202301.zip".to_string();
        let url2 = "https://example.com/202302.zip".to_string();
        links.insert("202301".to_string(), url1.clone());
        links.insert("202302".to_string(), url2.clone());

        let result = filter_periods_by_range(&links, None, None);
        let filtered = result.unwrap();

        assert_eq!(filtered.get("202301"), Some(&url1));
        assert_eq!(filtered.get("202302"), Some(&url2));
    }

    #[test]
    fn test_filter_with_non_numeric_periods() {
        let mut links = BTreeMap::new();
        links.insert(
            "invalid".to_string(),
            "https://example.com/invalid.zip".to_string(),
        );
        links.insert(
            "202301".to_string(),
            "https://example.com/202301.zip".to_string(),
        );

        let result = filter_periods_by_range(&links, None, None);
        assert!(result.is_ok());
        let filtered = result.unwrap();

        // Non-numeric periods are silently skipped
        assert_eq!(filtered.len(), 1);
        assert!(filtered.contains_key("202301"));
    }

    #[test]
    fn test_filter_start_greater_than_end() {
        let links = create_test_links();
        // This should return empty because no periods fall in the range
        let result = filter_periods_by_range(&links, Some("202305"), Some("202301"));

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_parse_zip_links_basic() {
        let html = r#"
            <html>
            <body>
              <a href="files/data_202301.zip">202301</a>
              <a href="/downloads/data_202302.zip">202302</a>
              <a href="https://other.example.com/attachments/data_202303.zip">202303</a>
              <a href="not_a_zip.txt">skip</a>
            </body>
            </html>
        "#;

        let base = Url::parse("https://example.com/path/").expect("base url");
        let result = parse_zip_links(html, &base).expect("parse succeeds");

        // Should contain the three detected periods with absolute URLs
        assert_eq!(result.get("202301").unwrap(), "https://example.com/path/files/data_202301.zip");
        assert_eq!(result.get("202302").unwrap(), "https://example.com/downloads/data_202302.zip");
        assert_eq!(result.get("202303").unwrap(), "https://other.example.com/attachments/data_202303.zip");
    }
}
