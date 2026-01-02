use crate::errors::{AppError, AppResult};
use crate::models::ProcurementType;
use crate::ui;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Result type for parallel download tasks.
/// Returns (filename, success, optional_error_message)
type DownloadTaskResult = Result<(String, bool, Option<String>), AppError>;

/// Extracts HTTP status code from error message if present.
///
/// Looks for the pattern "HTTP {status_code}:" in the error message.
/// Returns `Some(status_code)` if found, `None` otherwise.
fn extract_status_code(msg: &str) -> Option<u16> {
    let prefix = "HTTP ";
    if let Some(start) = msg.find(prefix) {
        let start = start + prefix.len();
        let end = msg[start..].find(':').unwrap_or(msg[start..].len());
        msg[start..start + end].trim().parse().ok()
    } else {
        None
    }
}

/// Determines if an error should trigger a retry attempt.
///
/// Returns `true` for retryable errors (network errors, timeouts, 5xx HTTP status codes).
/// Returns `false` for non-retryable errors (4xx client errors, I/O errors, validation errors).
fn should_retry(error: &AppError) -> bool {
    match error {
        AppError::NetworkError(msg) => {
            // Extract status code from message if present
            if let Some(status_code) = extract_status_code(msg) {
                // 4xx = client error, don't retry
                // 5xx = server error, retry
                status_code >= 500
            } else {
                // No status code means network/timeout error - retry by default
                // Legacy string matching fallback for older error formats
                !msg.contains("400")
                    && !msg.contains("401")
                    && !msg.contains("403")
                    && !msg.contains("404")
                    && !msg.contains("client error")
            }
        }
        AppError::IoError(_) => false,       // Don't retry I/O errors
        AppError::ParseError(_) => false,    // Don't retry parse errors
        AppError::UrlError(_) => false,      // Don't retry URL errors
        AppError::RegexError(_) => false,    // Don't retry regex errors
        AppError::SelectorError(_) => false, // Don't retry selector errors
        AppError::PeriodValidationError { .. } => false, // Don't retry validation errors
        AppError::InvalidInput(_) => false,  // Don't retry invalid input errors
    }
}

/// Configuration for retry behavior.
pub(crate) struct RetryConfig {
    max_retries: u32,
    initial_delay_ms: u64,
    max_delay_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 10000,
        }
    }
}

/// Calculates exponential backoff delay in milliseconds.
///
/// Formula: `min(initial_delay * 2^attempt, max_delay)`
fn calculate_backoff(attempt: u32, config: &RetryConfig) -> u64 {
    let delay = config.initial_delay_ms * 2_u64.pow(attempt);
    delay.min(config.max_delay_ms)
}

/// Internal retry function that takes RetryConfig directly.
pub(crate) async fn download_with_retry_internal(
    client: &reqwest::Client,
    url: &str,
    tmp_path: &Path,
    file_path: &Path,
    filename: &str,
    retry_config: &RetryConfig,
) -> AppResult<()> {
    let mut last_error: Option<AppError> = None;

    for attempt in 0..=retry_config.max_retries {
        match download_single_file(client, url, tmp_path, file_path, filename).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if attempt < retry_config.max_retries && should_retry(&e) {
                    let delay_ms = calculate_backoff(attempt, retry_config);
                    warn!(
                        filename = filename,
                        attempt = attempt + 1,
                        max_retries = retry_config.max_retries + 1,
                        delay_ms = delay_ms,
                        error = %e,
                        "Retrying download after error"
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                    last_error = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        AppError::NetworkError(format!(
            "Download failed after {} retries (no error recorded)",
            retry_config.max_retries + 1
        ))
    }))
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
    // Send request and handle send errors (network/timeout errors)
    let response = client.get(url).send().await.map_err(|e| {
        // For send errors, these are typically network/timeout errors (retryable)
        AppError::NetworkError(format!("Failed to download {filename}: {e}"))
    })?;

    // Check status before error_for_status (which converts 4xx/5xx to errors)
    let status = response.status();
    let mut response = response.error_for_status().map_err(|e| {
        // Include status code in error message for retry logic
        let status_code = status.as_u16();
        AppError::NetworkError(format!(
            "HTTP {status_code}: Failed to download {filename}: {e}"
        ))
    })?;

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
pub async fn download_files(
    client: &reqwest::Client,
    filtered_links: &std::collections::BTreeMap<String, String>,
    proc_type: &ProcurementType,
    config: &crate::config::ResolvedConfig,
) -> AppResult<()> {
    let download_dir = proc_type.download_dir(config);
    // Create directory if it doesn't exist
    if !download_dir.exists() {
        fs::create_dir_all(&download_dir)
            .await
            .map_err(|e| AppError::IoError(format!("Failed to create directory: {e}")))?;
    }

    // Count files that need downloading (excluding existing ones)
    // Collect as owned values to avoid lifetime issues with spawned tasks
    let files_to_download: Vec<(String, String)> = filtered_links
        .iter()
        .filter(|(period, _)| {
            let file_path = download_dir.join(format!("{period}.zip"));
            !file_path.exists()
        })
        .map(|(period, url)| (period.clone(), url.clone()))
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
    let pb = ui::create_progress_bar(total_files as u64)?;

    info!(
        total = total_files,
        skipped = skipped_count,
        "Starting download"
    );

    // Create semaphore to limit concurrent downloads
    let concurrent_downloads = config.concurrent_downloads;
    let semaphore = Arc::new(Semaphore::new(concurrent_downloads));
    let client = Arc::new(client.clone());
    let download_dir_path = download_dir.clone();
    let download_dir_arc = Arc::new(download_dir_path);
    let pb = Arc::new(pb);

    // Extract retry config values before moving into async blocks
    let retry_max_retries = config.max_retries;
    let retry_initial_delay_ms = config.retry_initial_delay_ms;
    let retry_max_delay_ms = config.retry_max_delay_ms;

    // Pre-allocate errors Vec (usually small, but could accumulate)
    let mut errors = Vec::with_capacity(10);
    let mut success_count = 0;

    // Spawn download tasks with bounded concurrency
    let mut handles: Vec<JoinHandle<DownloadTaskResult>> = Vec::with_capacity(total_files);

    for (period, url) in files_to_download.iter() {
        let filename = format!("{period}.zip");

        // Clone Arc references and owned values for the task
        let semaphore = semaphore.clone();
        let client = client.clone();
        let download_dir = download_dir_arc.clone();
        let pb = pb.clone();
        let period = period.clone();
        let url = url.clone();
        let filename_for_task = filename.clone();

        // Clone retry config values for this task
        let max_retries = retry_max_retries;
        let initial_delay_ms = retry_initial_delay_ms;
        let max_delay_ms = retry_max_delay_ms;

        // Spawn task that will acquire semaphore permit before downloading
        let handle = tokio::spawn(async move {
            // Create paths inside the task
            let file_path = download_dir.join(&filename_for_task);
            let tmp_path = download_dir.join(format!("{period}.zip.part"));

            // Acquire permit (will wait if 4 downloads are already in progress)
            let _permit = semaphore.acquire().await.map_err(|e| {
                AppError::IoError(format!("Failed to acquire semaphore permit: {e}"))
            })?;

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
            pb.set_message(format!("Downloading {filename_for_task}..."));

            // Attempt download with retry logic
            // Create RetryConfig from cloned values
            let retry_config = RetryConfig {
                max_retries,
                initial_delay_ms,
                max_delay_ms,
            };

            let result = download_with_retry_internal(
                &client,
                &url,
                &tmp_path,
                &file_path,
                &filename_for_task,
                &retry_config,
            )
            .await;

            // Update progress bar based on result
            match &result {
                Ok(_) => {
                    pb.set_message(format!("Completed {filename_for_task}"));
                    Ok((filename_for_task, true, None))
                }
                Err(e) => {
                    let error_msg = format!("Failed to download {filename_for_task}: {e}");
                    warn!(
                        filename = filename_for_task,
                        error = %e,
                        "Failed to download file"
                    );
                    pb.set_message(format!("Failed {filename_for_task}"));
                    Ok((filename_for_task, false, Some(error_msg)))
                }
            }
        });

        handles.push(handle);
    }

    // Await all tasks and collect results
    for handle in handles {
        // Update progress bar for each completed download
        pb.inc(1);

        match handle.await {
            Ok(Ok((_filename, success, error_msg))) => {
                if success {
                    success_count += 1;
                } else if let Some(msg) = error_msg {
                    errors.push(msg);
                }
            }
            Ok(Err(e)) => {
                errors.push(format!("Task error: {e}"));
            }
            Err(e) => {
                errors.push(format!("Task join error: {e}"));
            }
        }
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
