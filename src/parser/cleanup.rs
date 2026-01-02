use crate::errors::AppResult;
use std::collections::BTreeMap;
use tracing::{info, warn};

/// Deletes ZIP files and extracted directories after processing.
///
/// This function removes temporary files created during the download and extraction
/// phases, keeping only the final Parquet files. It's typically called after
/// successful parsing to free up disk space.
///
/// # Behavior
///
/// For each period in `target_links`, this function:
/// - Deletes the ZIP file: `{extract_dir}/{period}.zip`
/// - Deletes the extracted directory: `{extract_dir}/{period}/` (recursively removes all XML/Atom files)
///
/// # Arguments
///
/// * `target_links` - Map of period strings to URLs (determines which files to delete)
/// * `procurement_type` - Procurement type determining the extraction directory
/// * `should_cleanup` - If `false`, the function returns immediately without deleting anything
/// * `config` - Resolved configuration containing directory paths
///
/// # Error Handling
///
/// Individual deletion errors are logged as warnings but do not fail the entire operation.
/// The function continues processing remaining files even if some deletions fail.
pub async fn cleanup_files(
    target_links: &BTreeMap<String, String>,
    procurement_type: &crate::models::ProcurementType,
    should_cleanup: bool,
    config: &crate::config::ResolvedConfig,
) -> AppResult<()> {
    if !should_cleanup {
        info!("Cleanup skipped (--cleanup=no)");
        return Ok(());
    }

    let extract_dir = procurement_type.extract_dir(config);
    if !extract_dir.exists() {
        info!("Extract directory does not exist, skipping cleanup");
        return Ok(());
    }

    info!("Starting cleanup phase");

    let mut zip_deleted = 0;
    let mut zip_errors = 0;
    let mut dir_deleted = 0;
    let mut dir_errors = 0;

    for period in target_links.keys() {
        // Delete ZIP file
        let zip_path = extract_dir.join(format!("{period}.zip"));
        if zip_path.exists() {
            match tokio::fs::remove_file(&zip_path).await {
                Ok(_) => {
                    zip_deleted += 1;
                }
                Err(e) => {
                    zip_errors += 1;
                    warn!(
                        zip_file = %zip_path.display(),
                        period = period,
                        error = %e,
                        "Failed to delete ZIP file"
                    );
                }
            }
        }

        // Delete extracted directory (contains XML/Atom files)
        let extract_dir_path = extract_dir.join(period);
        if extract_dir_path.exists() {
            match tokio::fs::remove_dir_all(&extract_dir_path).await {
                Ok(_) => {
                    dir_deleted += 1;
                }
                Err(e) => {
                    dir_errors += 1;
                    warn!(
                        extract_dir = %extract_dir_path.display(),
                        period = period,
                        error = %e,
                        "Failed to delete extracted directory"
                    );
                }
            }
        }
    }

    info!(
        zip_deleted = zip_deleted,
        zip_errors = zip_errors,
        dir_deleted = dir_deleted,
        dir_errors = dir_errors,
        "Cleanup completed"
    );

    Ok(())
}
