use crate::errors::{AppError, AppResult};
use crate::models::ProcurementType;
use crate::ui;
use rayon::{prelude::*, ThreadPoolBuilder};
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{copy, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use zip::ZipArchive;

/// Extracts ZIP files from the specified directory into subdirectories.
///
/// This function processes ZIP files that correspond to periods in `target_links`.
/// For each period (e.g., "202301"), it looks for a corresponding ZIP file (`202301.zip`)
/// in the extraction directory and extracts its contents into a subdirectory named
/// after the period (`202301/`).
///
/// # Behavior
///
/// - **Skip existing**: If an extraction directory already exists for a period, that
///   ZIP file is skipped.
/// - **Missing files**: Missing ZIP files are logged as warnings but don't fail the
///   operation.
/// - **Progress tracking**: A progress bar is displayed during extraction.
///
/// # Arguments
///
/// * `target_links` - Map of period strings to URLs (used to determine which ZIPs to extract)
/// * `procurement_type` - Procurement type determining the extraction directory
///
/// # Directory Structure
///
/// For a period "202301", the function expects:
/// - Input: `{extract_dir}/202301.zip`
/// - Output: `{extract_dir}/202301/` (contains extracted XML/Atom files)
///
/// # Errors
///
/// Returns an error if:
/// - The extraction directory doesn't exist
/// - ZIP file extraction fails for any file
///
pub async fn extract_all_zips(
    target_links: &BTreeMap<String, String>,
    procurement_type: &ProcurementType,
    config: &crate::config::ResolvedConfig,
) -> AppResult<()> {
    let extract_dir = procurement_type.extract_dir(config);
    if !extract_dir.exists() {
        return Err(AppError::IoError(format!(
            "Directory does not exist: {}",
            extract_dir.display()
        )));
    }

    // Collect ZIP files that need extraction
    // Pre-allocate with known upper bound (bounded by target_links.len())
    let capacity = target_links.len();
    let mut zips_to_extract: Vec<PathBuf> = Vec::with_capacity(capacity);
    let mut missing_zips = Vec::with_capacity(capacity);

    for period in target_links.keys() {
        let zip_path = extract_dir.join(format!("{period}.zip"));
        if !zip_path.exists() {
            missing_zips.push((period.clone(), zip_path));
            continue;
        }

        // Check if extraction directory already exists
        let extract_dir_path = zip_path
            .parent()
            .ok_or_else(|| {
                AppError::InvalidInput(format!(
                    "ZIP file has no parent directory: {}",
                    zip_path.display()
                ))
            })?
            .join(period);

        if !extract_dir_path.exists() {
            zips_to_extract.push(zip_path);
        }
    }

    let total_zips = zips_to_extract.len();
    let skipped_count = target_links.len() - total_zips - missing_zips.len();

    if total_zips == 0 {
        info!(
            total = target_links.len(),
            skipped = skipped_count,
            missing = missing_zips.len(),
            "All ZIP files already extracted, skipping extraction"
        );
        return Ok(());
    }

    // Log warnings for missing ZIP files
    for (period, zip_path) in &missing_zips {
        warn!(
            zip_file = %zip_path.display(),
            period = period,
            "ZIP file not found, skipping"
        );
    }

    // Create progress bar and atomic counter for thread-safe progress tracking
    let pb = ui::create_progress_bar(total_zips as u64)?;
    info!(
        total = total_zips,
        skipped = skipped_count,
        missing = missing_zips.len(),
        "Starting extraction"
    );

    let progress_counter = Arc::new(AtomicUsize::new(0));
    let monitor_pb = pb.clone();
    let monitor_counter = progress_counter.clone();
    let monitor_total = total_zips as u64;
    let monitor_handle = tokio::spawn(async move {
        use tokio::time::sleep;

        loop {
            let current = monitor_counter.load(Ordering::Relaxed) as u64;
            monitor_pb.set_position(current);
            if current >= monitor_total {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    });

    let cpu_count = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1);
    let thread_count = cpu_count.saturating_mul(2);
    let rayon_pool = ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .map_err(|e| AppError::IoError(format!("Failed to configure rayon thread pool: {e}")))?;

    let counter_for_workers = progress_counter.clone();

    // Run parallel extraction using rayon within spawn_blocking
    let results = tokio::task::spawn_blocking(move || {
        rayon_pool.install(|| {
            zips_to_extract
                .par_iter()
                .map(|zip_path| {
                    let result = extract_zip_sync(zip_path);
                    counter_for_workers.fetch_add(1, Ordering::Relaxed);
                    (zip_path.clone(), result)
                })
                .collect::<Vec<(PathBuf, AppResult<()>)>>()
        })
    })
    .await
    .map_err(|e| AppError::IoError(format!("Task join error: {e}")))?;

    monitor_handle
        .await
        .map_err(|e| AppError::IoError(format!("Progress monitor error: {e}")))?;

    pb.finish_with_message(format!("Extracted {total_zips} ZIP file(s)"));

    // Collect errors
    let mut errors = Vec::new();
    for (zip_path, result) in results {
        if let Err(e) = result {
            let error_msg = format!("Failed to extract {}: {}", zip_path.display(), e);
            warn!(
                zip_file = %zip_path.display(),
                error = %e,
                "Failed to extract ZIP file"
            );
            errors.push(error_msg);
        }
    }

    if !errors.is_empty() {
        return Err(AppError::IoError(format!(
            "Failed to extract {} ZIP file(s): {}",
            errors.len(),
            errors.join("; ")
        )));
    }

    if skipped_count > 0 {
        debug!(skipped = skipped_count, "Skipped already extracted files");
    }

    info!(
        extracted = total_zips,
        skipped = skipped_count,
        missing = missing_zips.len(),
        "Extraction completed"
    );

    Ok(())
}

/// Synchronous function to extract a single ZIP file.
/// This is used by rayon for parallel processing.
fn extract_zip_sync(zip_path: &Path) -> AppResult<()> {
    let zip_file_name = zip_path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| {
            AppError::InvalidInput(format!("Invalid ZIP file name: {}", zip_path.display()))
        })?;

    let extract_dir = zip_path
        .parent()
        .ok_or_else(|| {
            AppError::InvalidInput(format!(
                "ZIP file has no parent directory: {}",
                zip_path.display()
            ))
        })?
        .join(zip_file_name);

    // Skip if extraction directory already exists
    if extract_dir.exists() {
        debug!(
            zip_file = %zip_path.display(),
            extract_dir = %extract_dir.display(),
            "Skipping extraction, directory already exists"
        );
        return Ok(());
    }

    // Create extraction directory
    std::fs::create_dir_all(&extract_dir).map_err(|e| {
        AppError::IoError(format!(
            "Failed to create extraction directory {}: {}",
            extract_dir.display(),
            e
        ))
    })?;

    // Open and extract ZIP file
    let file = File::open(zip_path).map_err(|e| {
        AppError::IoError(format!(
            "Failed to open ZIP file {}: {}",
            zip_path.display(),
            e
        ))
    })?;

    let mut archive = ZipArchive::new(file).map_err(|e| {
        AppError::ParseError(format!(
            "Failed to read ZIP archive {}: {}",
            zip_path.display(),
            e
        ))
    })?;

    let mut entries = Vec::with_capacity(archive.len());
    let mut created_dirs = HashSet::new();

    for i in 0..archive.len() {
        let file = archive.by_index(i).map_err(|e| {
            AppError::ParseError(format!(
                "Failed to read file {} from ZIP {}: {}",
                i,
                zip_path.display(),
                e
            ))
        })?;

        let out_path = match file.enclosed_name() {
            Some(path) => extract_dir.join(path),
            None => continue,
        };

        if file.name().ends_with('/') {
            continue;
        }

        if let Some(parent) = out_path.parent() {
            if created_dirs.insert(parent.to_path_buf()) {
                std::fs::create_dir_all(parent).map_err(|e| {
                    AppError::IoError(format!(
                        "Failed to create directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
        }

        entries.push((i, out_path));
    }

    drop(archive);

    let zip_path_arc = Arc::new(zip_path.to_path_buf());
    entries
        .par_iter()
        .map(|(index, out_path)| {
            let zip_path = zip_path_arc.clone();
            let file = File::open(&*zip_path).map_err(|e| {
                AppError::IoError(format!(
                    "Failed to open ZIP file {}: {}",
                    zip_path.display(),
                    e
                ))
            })?;

            let mut archive = ZipArchive::new(file).map_err(|e| {
                AppError::ParseError(format!(
                    "Failed to read ZIP archive {}: {}",
                    zip_path.display(),
                    e
                ))
            })?;

            let mut file = archive.by_index(*index).map_err(|e| {
                AppError::ParseError(format!(
                    "Failed to read file {} from ZIP {}: {}",
                    index,
                    zip_path.display(),
                    e
                ))
            })?;

            let out_file = std::fs::File::create(out_path).map_err(|e| {
                AppError::IoError(format!(
                    "Failed to create file {}: {}",
                    out_path.display(),
                    e
                ))
            })?;

            let mut writer = BufWriter::with_capacity(32 * 1024, out_file);
            copy(&mut file, &mut writer).map_err(|e| {
                AppError::IoError(format!(
                    "Failed to copy file from ZIP {} to {}: {}",
                    zip_path.display(),
                    out_path.display(),
                    e
                ))
            })?;
            writer.flush().map_err(|e| {
                AppError::IoError(format!(
                    "Failed to flush file {}: {}",
                    out_path.display(),
                    e
                ))
            })?;

            Ok(())
        })
        .collect::<AppResult<Vec<()>>>()?;

    Ok(())
}
