use crate::errors::{AppError, AppResult};
use indicatif::{ProgressBar, ProgressStyle};

/// Creates a progress bar with the standard application styling.
///
/// This helper function centralizes the progress bar configuration used throughout
/// the application, ensuring consistent styling and reducing code duplication.
///
/// # Arguments
///
/// * `total` - Total number of items to process
///
/// # Returns
///
/// Returns a configured `ProgressBar` ready for use, or an error if template creation fails.
///
pub fn create_progress_bar(total: u64) -> AppResult<ProgressBar> {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} {msg}",
            )
            .map_err(|e| AppError::IoError(format!("Failed to create progress bar template: {e}")))?
            .progress_chars("#>-"),
    );
    Ok(pb)
}
