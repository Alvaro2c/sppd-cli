use crate::constants::{MINOR_CONTRACTS, PERIOD_REGEX_PATTERN, PUBLIC_TENDERS, ZIP_LINK_SELECTOR};
use crate::errors::{AppError, AppResult};
use crate::models::ProcurementType;
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use reqwest;
use scraper::{Html, Selector};
use std::collections::BTreeMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::OnceLock;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};
use url::Url;

/// Cached regex for extracting period identifiers from ZIP filenames.
/// Compiled once at initialization for performance.
static PERIOD_REGEX: OnceLock<Regex> = OnceLock::new();

/// Cached CSS selector for ZIP file links.
/// Compiled once at initialization for performance.
static ZIP_LINK_SELECTOR_CACHED: OnceLock<Selector> = OnceLock::new();

/// Fetches all available ZIP file links from both procurement data sources.
///
/// This function sequentially fetches links from both the minor contracts and
/// public tenders data source pages. It parses HTML to extract ZIP file links
/// and extracts period identifiers (e.g., "202301") from filenames.
///
/// # Returns
///
/// Returns a tuple containing maps of period strings to download URLs:
/// - **First element**: Minor contracts links (period -> URL)
/// - **Second element**: Public tenders links (period -> URL)
///
/// # Errors
///
/// Returns an error if:
/// - Network requests fail
/// - HTML parsing fails
/// - URLs cannot be parsed
///
/// # Example
///
/// ```no_run
/// use sppd_cli::downloader;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let (minor_links, public_links) = downloader::fetch_all_links().await?;
/// println!("Found {} minor contract periods", minor_links.len());
/// println!("Found {} public tender periods", public_links.len());
/// # Ok(())
/// # }
/// ```
pub async fn fetch_all_links() -> AppResult<(BTreeMap<String, String>, BTreeMap<String, String>)> {
    let client = reqwest::Client::new();
    // Sequential fetch: simple and reliable for two landing pages.
    info!("Fetching minor contracts links");
    let minor_links = fetch_zip(&client, MINOR_CONTRACTS).await?;
    info!(
        periods_found = minor_links.len(),
        "Minor contracts links fetched"
    );

    info!("Fetching public tenders links");
    let public_links = fetch_zip(&client, PUBLIC_TENDERS).await?;
    info!(
        periods_found = public_links.len(),
        "Public tenders links fetched"
    );

    Ok((minor_links, public_links))
}

/// Fetches ZIP file links from a single procurement data page.
///
/// Downloads the HTML content from the given URL and parses it to extract
/// all ZIP file download links. Period identifiers are extracted from filenames
/// using a regex pattern that matches `_YYYYMM.zip` or similar formats.
///
/// # Arguments
///
/// * `client` - HTTP client to use for the request
/// * `input_url` - URL of the page containing ZIP file links (e.g., the minor contracts
///   or public tenders landing page)
///
/// # Returns
///
/// A map from period strings (e.g., "202301") to absolute download URLs.
///
/// # Errors
///
/// Returns an error if:
/// - The HTTP request fails
/// - The URL cannot be parsed
/// - HTML parsing fails
///
/// # Example
///
/// ```no_run
/// use sppd_cli::downloader;
/// use reqwest::Client;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new();
/// let url = "https://www.hacienda.gob.es/es-es/gobiernoabierto/datos%20abiertos/paginas/contratosmenores.aspx";
/// let links = downloader::fetch_zip(&client, url).await?;
/// println!("Found {} periods", links.len());
/// # Ok(())
/// # }
/// ```
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

/// Parses HTML content and extracts ZIP file links, extracting period identifiers from filenames.
///
/// This function searches for all `<a>` tags with `href` attributes ending in `.zip`,
/// extracts period identifiers from filenames using a regex pattern (e.g., `_202301.zip`),
/// and resolves relative URLs to absolute URLs using the base URL.
///
/// # Returns
///
/// Returns a map where keys are period strings (e.g., "202301") and values are absolute URLs.
///
/// # Example
///
/// ```
/// use sppd_cli::downloader::parse_zip_links;
/// use url::Url;
///
/// # fn main() -> Result<(), sppd_cli::errors::AppError> {
/// let html = r#"
///     <html>
///         <body>
///             <a href="data_202301.zip">January 2023</a>
///             <a href="data_202302.zip">February 2023</a>
///         </body>
///     </html>
/// "#;
/// let base = Url::parse("https://example.com/downloads/")?;
/// let links = parse_zip_links(html, &base)?;
///
/// assert_eq!(links.get("202301"), Some(&"https://example.com/downloads/data_202301.zip".to_string()));
/// assert_eq!(links.get("202302"), Some(&"https://example.com/downloads/data_202302.zip".to_string()));
/// # Ok(())
/// # }
/// ```
pub fn parse_zip_links(html: &str, base_url: &Url) -> AppResult<BTreeMap<String, String>> {
    let document = Html::parse_document(html);

    let mut links: BTreeMap<String, String> = BTreeMap::new();

    let selector = ZIP_LINK_SELECTOR_CACHED.get_or_init(|| {
        Selector::parse(ZIP_LINK_SELECTOR).expect("ZIP_LINK_SELECTOR is a valid CSS selector")
    });

    let period_regex = PERIOD_REGEX.get_or_init(|| {
        Regex::new(PERIOD_REGEX_PATTERN).expect("PERIOD_REGEX_PATTERN is a valid regex pattern")
    });

    for url in document
        .select(selector)
        .filter_map(|el| el.value().attr("href"))
        .filter_map(|href| base_url.join(href).ok())
    {
        if let Some(filename) = url.path_segments().and_then(|mut s| s.next_back()) {
            if let Some(m) = period_regex.captures(filename).and_then(|c| c.get(1)) {
                links.insert(m.as_str().to_string(), url.to_string());
            }
        }
    }

    Ok(links)
}

/// Validates that a period string matches the expected format (YYYY or YYYYMM).
///
/// This function checks that the period contains only ASCII digits and has
/// exactly 4 digits (YYYY) or 6 digits (YYYYMM).
///
/// # Arguments
///
/// * `period` - Period string to validate
///
/// # Returns
///
/// Returns `Ok(())` if the period format is valid, or `InvalidInput` error otherwise.
///
/// # Example
///
/// ```
/// use sppd_cli::downloader::validate_period_format;
///
/// assert!(validate_period_format("2023").is_ok());      // YYYY format
/// assert!(validate_period_format("202301").is_ok());    // YYYYMM format
/// assert!(validate_period_format("202").is_err());      // Too short
/// assert!(validate_period_format("20230101").is_err()); // Too long
/// assert!(validate_period_format("abcd").is_err());     // Non-numeric
/// ```
pub fn validate_period_format(period: &str) -> AppResult<()> {
    if period.is_empty() {
        return Err(AppError::InvalidInput(
            "Period must be YYYY or YYYYMM format (4 or 6 digits), got empty string".to_string(),
        ));
    }
    if !period.chars().all(|c| c.is_ascii_digit()) {
        return Err(AppError::InvalidInput(format!(
            "Period must contain only digits, got: {period}"
        )));
    }
    match period.len() {
        4 | 6 => Ok(()),
        _ => Err(AppError::InvalidInput(format!(
            "Period must be YYYY or YYYYMM format (4 or 6 digits), got: {} ({} digits)",
            period,
            period.len()
        ))),
    }
}

/// Filters links by period range, validating that specified periods exist.
///
/// This function filters a map of period-to-URL links based on a start and/or end period.
/// Periods are compared numerically (e.g., "202301" < "202302"). The range is inclusive
/// on both ends.
///
/// # Arguments
///
/// * `links` - Map of period strings to URLs to filter
/// * `start_period` - Optional start period (inclusive). If `None`, no lower bound.
/// * `end_period` - Optional end period (inclusive). If `None`, no upper bound.
///
/// # Returns
///
/// A filtered map containing only periods within the specified range.
///
/// # Errors
///
/// Returns `InvalidInput` if `start_period` or `end_period` has an invalid format
/// (not YYYY or YYYYMM). Returns `PeriodValidationError` if the period format is valid
/// but doesn't exist in the `links` map.
///
/// # Example
///
/// ```
/// use sppd_cli::downloader::filter_periods_by_range;
/// use std::collections::BTreeMap;
///
/// # fn main() -> Result<(), sppd_cli::errors::AppError> {
/// let mut links = BTreeMap::new();
/// links.insert("202301".to_string(), "https://example.com/202301.zip".to_string());
/// links.insert("202302".to_string(), "https://example.com/202302.zip".to_string());
/// links.insert("202303".to_string(), "https://example.com/202303.zip".to_string());
///
/// // Filter from start period only
/// let filtered = filter_periods_by_range(&links, Some("202302"), None)?;
/// assert_eq!(filtered.len(), 2); // 202302, 202303
///
/// // Filter with both start and end
/// let filtered = filter_periods_by_range(&links, Some("202301"), Some("202302"))?;
/// assert_eq!(filtered.len(), 2); // 202301, 202302
///
/// // Filter all (no constraints)
/// let filtered = filter_periods_by_range(&links, None, None)?;
/// assert_eq!(filtered.len(), 3);
/// # Ok(())
/// # }
/// ```
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

    // Validate that specified periods have correct format and exist in links
    let validate_period = |period: Option<&str>| -> AppResult<()> {
        if let Some(p) = period {
            // First validate the format
            validate_period_format(p)?;
            // Then check if it exists in links
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

/// Downloads a single ZIP file.
///
/// This is a helper function that performs the download of a single file,
/// used by `download_files` to enable error collection and continuation.
async fn download_single_file(
    client: &reqwest::Client,
    url: &str,
    tmp_path: &Path,
    file_path: &Path,
    filename: &str,
) -> AppResult<()> {
    let mut response = client
        .get(url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| AppError::NetworkError(format!("Failed to download {filename}: {e}")))?;

    let mut file = File::create(tmp_path).await.map_err(|e| {
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
    fs::rename(tmp_path, file_path).await.map_err(|e| {
        AppError::IoError(format!(
            "Failed to rename temp file {} to {}: {}",
            tmp_path.display(),
            file_path.display(),
            e
        ))
    })?;

    Ok(())
}

/// Downloads ZIP files to the appropriate directory based on procurement type.
///
/// This function downloads ZIP files from the provided URLs to the directory
/// specified by the procurement type (e.g., `data/tmp/mc` or `data/tmp/pt`).
///
/// # Behavior
///
/// - **Atomic downloads**: Files are downloaded to temporary `.part` files and
///   atomically renamed when complete, preventing partial downloads.
/// - **Skip existing**: Files that already exist are automatically skipped.
/// - **Progress tracking**: A progress bar is displayed during downloads.
///
/// # Arguments
///
/// * `client` - HTTP client for making requests
/// * `filtered_links` - Map of period strings to download URLs (typically from
///   `filter_periods_by_range()`)
/// * `proc_type` - Procurement type determining the download directory
///
/// # Errors
///
/// Returns an error if:
/// - Directory creation fails
/// - Network requests fail
/// - File I/O operations fail
///
/// # Example
///
/// ```no_run
/// use sppd_cli::{downloader, models::ProcurementType};
/// use reqwest::Client;
/// use std::collections::BTreeMap;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new();
/// let mut links = BTreeMap::new();
/// links.insert("202301".to_string(), "https://example.com/data_202301.zip".to_string());
/// downloader::download_files(&client, &links, &ProcurementType::PublicTenders).await?;
/// # Ok(())
/// # }
/// ```
pub async fn download_files(
    client: &reqwest::Client,
    filtered_links: &BTreeMap<String, String>,
    proc_type: &ProcurementType,
) -> AppResult<()> {
    let download_dir = proc_type.download_dir();
    // Create directory if it doesn't exist
    if !download_dir.exists() {
        fs::create_dir_all(&download_dir)
            .await
            .map_err(|e| AppError::IoError(format!("Failed to create directory: {e}")))?;
    }

    // Count files that need downloading (excluding existing ones)
    let files_to_download: Vec<_> = filtered_links
        .iter()
        .filter(|(period, _)| {
            let file_path = download_dir.join(format!("{period}.zip"));
            !file_path.exists()
        })
        .collect();

    let total_files = files_to_download.len();
    let skipped_count = filtered_links.len() - total_files;

    if total_files == 0 {
        info!(
            count = filtered_links.len(),
            "All files already exist, skipping downloads"
        );
        return Ok(());
    }

    // Create progress bar
    let pb = ProgressBar::new(total_files as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
    );

    info!(
        total = total_files,
        skipped = skipped_count,
        "Starting download"
    );

    // Pre-allocate errors Vec (usually small, but could accumulate)
    let mut errors = Vec::with_capacity(10);
    let mut success_count = 0;

    for (period, url) in files_to_download.iter() {
        let filename = format!("{period}.zip");
        let file_path = download_dir.join(&filename);

        // Temporary partial file (atomic rename after complete)
        let tmp_path = download_dir.join(format!("{period}.zip.part"));

        // Remove stale tmp file if present (best-effort)
        if tmp_path.exists() {
            if let Err(e) = fs::remove_file(&tmp_path).await {
                warn!(
                    file_path = %tmp_path.display(),
                    error = %e,
                    "Failed to remove stale temp file"
                );
            }
        }

        // Update progress bar message
        pb.set_message(format!("Downloading {filename}..."));

        // Attempt download and collect errors instead of returning early
        if let Err(e) = download_single_file(client, url, &tmp_path, &file_path, &filename).await {
            let error_msg = format!("Failed to download {filename}: {e}");
            warn!(
                filename = filename,
                error = %e,
                "Failed to download file"
            );
            errors.push(error_msg);
            pb.set_message(format!("Failed {filename}"));
        } else {
            success_count += 1;
            pb.set_message(format!("Completed {filename}"));
        }

        // Update progress bar regardless of success/failure
        pb.inc(1);
    }

    // Report results
    if errors.is_empty() {
        pb.finish_with_message(format!("Downloaded {success_count} file(s)"));
        info!(
            downloaded = success_count,
            skipped = skipped_count,
            "Download completed"
        );
    } else {
        pb.finish_with_message(format!(
            "Downloaded {success_count} file(s), {} failed",
            errors.len()
        ));
        info!(
            downloaded = success_count,
            failed = errors.len(),
            skipped = skipped_count,
            "Download completed with errors"
        );
    }

    if skipped_count > 0 {
        debug!(skipped = skipped_count, "Skipped existing files");
    }

    // Return error if any downloads failed
    if !errors.is_empty() {
        return Err(AppError::NetworkError(format!(
            "Failed to download {} file(s): {}",
            errors.len(),
            errors.join("; ")
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{filter_periods_by_range, parse_zip_links, validate_period_format};
    use crate::errors::AppError;
    use std::collections::BTreeMap;
    use url::Url;

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
    fn test_validate_period_format_valid_yyyy() {
        assert!(validate_period_format("2023").is_ok());
        assert!(validate_period_format("2024").is_ok());
        assert!(validate_period_format("1999").is_ok());
    }

    #[test]
    fn test_validate_period_format_valid_yyyymm() {
        assert!(validate_period_format("202301").is_ok());
        assert!(validate_period_format("202312").is_ok());
        assert!(validate_period_format("202401").is_ok());
    }

    #[test]
    fn test_validate_period_format_invalid_too_short() {
        let result = validate_period_format("202");
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("4 or 6 digits"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_period_format_invalid_too_long() {
        let result = validate_period_format("20230101");
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("4 or 6 digits"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_period_format_invalid_five_digits() {
        let result = validate_period_format("20231");
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("4 or 6 digits"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_period_format_invalid_non_numeric() {
        let result = validate_period_format("abcd");
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("only digits"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_period_format_invalid_mixed_chars() {
        let result = validate_period_format("2023ab");
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("only digits"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_period_format_empty_string() {
        let result = validate_period_format("");
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("empty string"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_filter_periods_invalid_format_start() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, Some("abc"), None);

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("only digits"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_filter_periods_invalid_format_end() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, None, Some("20231")); // 5 digits

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("4 or 6 digits"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
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
        assert_eq!(
            result.get("202301").unwrap(),
            "https://example.com/path/files/data_202301.zip"
        );
        assert_eq!(
            result.get("202302").unwrap(),
            "https://example.com/downloads/data_202302.zip"
        );
        assert_eq!(
            result.get("202303").unwrap(),
            "https://other.example.com/attachments/data_202303.zip"
        );
    }

    #[test]
    fn test_parse_zip_links_no_capture() {
        let html = r#"
            <html><body>
              <a href="files/data202301.zip">no underscore</a>
              <a href="files/data_abc.zip">non-numeric</a>
            </body></html>
        "#;

        let base = Url::parse("https://example.com/").expect("base url");
        let result = parse_zip_links(html, &base).expect("parse succeeds");
        // No valid numeric captures -> empty
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_zip_links_multiple_underscores_uses_last_capture() {
        let html = r#"
            <html><body>
              <a href="files/prefix_2023_202301.zip">multi</a>
            </body></html>
        "#;

        let base = Url::parse("https://example.com/").expect("base url");
        let result = parse_zip_links(html, &base).expect("parse succeeds");
        // Expect to capture the last numeric group (202301)
        assert_eq!(
            result.get("202301").unwrap(),
            "https://example.com/files/prefix_2023_202301.zip"
        );
    }

    #[test]
    fn test_parse_zip_links_duplicate_periods_last_wins() {
        let html = r#"
            <html><body>
              <a href="files/data_202301.zip">first</a>
              <a href="files/other_202301.zip">second</a>
            </body></html>
        "#;

        let base = Url::parse("https://example.com/").expect("base url");
        let result = parse_zip_links(html, &base).expect("parse succeeds");
        // BTreeMap insert will keep the last inserted value for the same key
        assert_eq!(
            result.get("202301").unwrap(),
            "https://example.com/files/other_202301.zip"
        );
    }

    #[test]
    fn test_parse_zip_links_relative_paths_resolve() {
        let html = r#"
            <html><body>
              <a href="./files/data_202304.zip">rel</a>
              <a href="../up/data_202305.zip">up</a>
            </body></html>
        "#;

        let base = Url::parse("https://example.com/path/sub/").expect("base url");
        let result = parse_zip_links(html, &base).expect("parse succeeds");
        assert_eq!(
            result.get("202304").unwrap(),
            "https://example.com/path/sub/files/data_202304.zip"
        );
        assert_eq!(
            result.get("202305").unwrap(),
            "https://example.com/path/up/data_202305.zip"
        );
    }
}
