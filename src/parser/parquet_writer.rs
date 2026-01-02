use crate::errors::{AppError, AppResult};
use crate::models::Entry;
use crate::ui;
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::sync::{Arc, Mutex};
use tempfile::NamedTempFile;
use tracing::info;

use super::file_finder::find_xmls;
use super::xml_parser::parse_xml;

/// Converts a vector of Entry structs into a Polars DataFrame.
///
/// This helper function creates a DataFrame from a slice of Entry structs,
/// ensuring consistent schema across all DataFrame creations.
/// Optimized to pre-allocate vectors and use take() instead of clone() where possible.
fn entries_to_dataframe(entries: Vec<Entry>) -> AppResult<DataFrame> {
    if entries.is_empty() {
        // Return empty DataFrame with correct schema
        return DataFrame::new(vec![
            Series::new("id", Vec::<Option<String>>::new()),
            Series::new("title", Vec::<Option<String>>::new()),
            Series::new("link", Vec::<Option<String>>::new()),
            Series::new("summary", Vec::<Option<String>>::new()),
            Series::new("updated", Vec::<Option<String>>::new()),
            Series::new("contract_folder_status", Vec::<Option<String>>::new()),
        ])
        .map_err(|e| AppError::ParseError(format!("Failed to create DataFrame: {e}")));
    }

    let len = entries.len();
    // Pre-allocate vectors with known capacity
    let mut ids = Vec::with_capacity(len);
    let mut titles = Vec::with_capacity(len);
    let mut links = Vec::with_capacity(len);
    let mut summaries = Vec::with_capacity(len);
    let mut updateds = Vec::with_capacity(len);
    let mut contract_folder_statuses = Vec::with_capacity(len);

    // Use take() to move values instead of cloning
    for entry in entries {
        ids.push(entry.id);
        titles.push(entry.title);
        links.push(entry.link);
        summaries.push(entry.summary);
        updateds.push(entry.updated);
        contract_folder_statuses.push(entry.contract_folder_status);
    }

    DataFrame::new(vec![
        Series::new("id", ids),
        Series::new("title", titles),
        Series::new("link", links),
        Series::new("summary", summaries),
        Series::new("updated", updateds),
        Series::new("contract_folder_status", contract_folder_statuses),
    ])
    .map_err(|e| AppError::ParseError(format!("Failed to create DataFrame: {e}")))
}

/// Parses XML/Atom files and converts them to Parquet format.
///
/// This function processes extracted XML/Atom files from the extraction directory,
/// parses them into `Entry` structures, and writes the results as Parquet files.
///
/// # Workflow
///
/// 1. Finds all subdirectories in the extraction directory that contain XML/Atom files
/// 2. Filters to only process subdirectories matching periods in `target_links`
/// 3. Parses all XML/Atom files in each matching subdirectory
/// 4. Converts parsed entries to a Polars DataFrame
/// 5. Writes the DataFrame as a Parquet file named after the period (e.g., `202301.parquet`)
///
/// # Directory Structure
///
/// The function expects the following structure:
/// - Input: `{extract_dir}/{period}/` (contains XML/Atom files)
/// - Output: `{parquet_dir}/{period}.parquet`
///
/// # Arguments
///
/// * `target_links` - Map of period strings to URLs (used to filter which periods to process)
/// * `procurement_type` - Procurement type determining the extract and parquet directories
/// * `batch_size` - Number of XML files to process per batch (affects memory usage)
/// * `config` - Resolved configuration containing directory paths
///
/// # Behavior
///
/// - **Filtering**: Only processes subdirectories whose names match keys in `target_links`
/// - **Skip empty**: Subdirectories with no entries are skipped (logged but not an error)
/// - **Progress tracking**: A progress bar is displayed during parsing
///
/// # Errors
///
/// Returns an error if:
/// - Directory creation fails
/// - XML parsing fails
/// - DataFrame creation fails
/// - Parquet file writing fails
pub fn parse_xmls(
    target_links: &BTreeMap<String, String>,
    procurement_type: &crate::models::ProcurementType,
    batch_size: usize,
    config: &crate::config::ResolvedConfig,
) -> AppResult<()> {
    let extract_dir = procurement_type.extract_dir(config);
    let parquet_dir = procurement_type.parquet_dir(config);

    // Create parquet directory if it doesn't exist
    fs::create_dir_all(&parquet_dir)
        .map_err(|e| AppError::IoError(format!("Failed to create parquet directory: {e}")))?;

    // Find all subdirectories with XML/atom files
    let subdirs = find_xmls(&extract_dir)?;

    // Filter subdirectories that match keys in target_links
    let subdirs_to_process: Vec<_> = subdirs
        .into_iter()
        .filter(|(subdir_name, _)| target_links.contains_key(subdir_name))
        .collect();

    let total_subdirs = subdirs_to_process.len();

    if total_subdirs == 0 {
        info!("No matching subdirectories found for parsing");
        return Ok(());
    }

    // Calculate total XML files across all periods for progress bar
    let total_xml_files: usize = subdirs_to_process
        .iter()
        .map(|(_, files)| files.len())
        .sum();

    // Create progress bar with total files instead of total periods
    let pb = ui::create_progress_bar(total_xml_files as u64)?;
    let progress_bar = Arc::new(Mutex::new(pb));

    info!(total = total_subdirs, "Starting XML parsing");

    let mut processed_count = 0;
    let mut skipped_count = 0;

    // Process each subdirectory
    for (subdir_name, xml_files) in subdirs_to_process {
        // Update progress bar message
        {
            let pb = progress_bar.lock().unwrap();
            pb.set_message(format!("Processing {subdir_name}..."));
        }

        // Process XML files in batches, writing each batch to temporary Parquet file
        let mut temp_files: Vec<NamedTempFile> = Vec::new();

        // Process XML files in batches (maintains memory safety)
        for xml_batch in xml_files.chunks(batch_size) {
            // Parse XML files in parallel within this batch with progress tracking
            let progress_bar_clone = progress_bar.clone();
            let batch_results: Vec<AppResult<Vec<Entry>>> = xml_batch
                .par_iter()
                .map(|xml_path| {
                    let result = parse_xml(xml_path);

                    // Update progress bar (thread-safe)
                    if let Ok(pb) = progress_bar_clone.lock() {
                        pb.inc(1);
                    }

                    result
                })
                .collect();

            // Collect entries, handling errors (fail-fast)
            let mut batch_entries = Vec::new();
            for result in batch_results {
                batch_entries.extend(result?);
            }

            if batch_entries.is_empty() {
                continue;
            }

            // Convert batch entries to DataFrame (takes ownership to avoid clones)
            let mut batch_df = entries_to_dataframe(batch_entries)?;

            // Create temporary Parquet file for this batch
            let temp_file = NamedTempFile::new()
                .map_err(|e| AppError::IoError(format!("Failed to create temp file: {e}")))?;

            // Write DataFrame to temporary Parquet file
            let mut file = File::create(temp_file.path()).map_err(|e| {
                AppError::IoError(format!("Failed to create temp Parquet file: {e}"))
            })?;

            ParquetWriter::new(&mut file)
                .finish(&mut batch_df)
                .map_err(|e| {
                    AppError::ParseError(format!("Failed to write temp Parquet file: {e}"))
                })?;

            // Store handle to prevent deletion (RAII will clean up later)
            temp_files.push(temp_file);
        }

        // Handle empty case
        if temp_files.is_empty() {
            skipped_count += 1;
            {
                let pb = progress_bar.lock().unwrap();
                pb.set_message(format!("Skipped {subdir_name} (no entries)"));
            }
            continue;
        }

        // Convert temp file handles to paths for DataFrame reading
        let temp_paths: Vec<String> = temp_files
            .iter()
            .map(|f| f.path().to_string_lossy().to_string())
            .collect();

        // Read all temporary Parquet files in parallel and concatenate them
        let dataframes: Vec<AppResult<DataFrame>> = temp_paths
            .par_iter()
            .map(|path| {
                let file = File::open(path).map_err(|e| {
                    AppError::IoError(format!("Failed to open temp Parquet file {path}: {e}"))
                })?;
                ParquetReader::new(file).finish().map_err(|e| {
                    AppError::ParseError(format!("Failed to read temp Parquet file {path}: {e}"))
                })
            })
            .collect();

        // Collect results, handling errors
        let dataframes: Vec<DataFrame> = dataframes.into_iter().collect::<AppResult<_>>()?;

        // Concatenate all DataFrames using optimized vstack
        let mut df = if dataframes.is_empty() {
            return Err(AppError::ParseError(
                "No DataFrames to concatenate".to_string(),
            ));
        } else if dataframes.len() == 1 {
            dataframes.into_iter().next().ok_or_else(|| {
                AppError::ParseError("Failed to get DataFrame from iterator".to_string())
            })?
        } else {
            // Use vstack with first DataFrame as base, then stack others
            // This is more efficient than sequential vstack on growing result
            let mut iter = dataframes.into_iter();
            let mut result = iter.next().ok_or_else(|| {
                AppError::ParseError("Failed to get first DataFrame from iterator".to_string())
            })?;
            // Collect remaining DataFrames and vstack them in batches for better performance
            let remaining: Vec<_> = iter.collect();
            if !remaining.is_empty() {
                // Stack all remaining at once if possible, otherwise do sequentially
                for other_df in remaining {
                    result = result.vstack(&other_df).map_err(|e| {
                        AppError::ParseError(format!("Failed to concatenate DataFrames: {e}"))
                    })?;
                }
            }
            result
        };

        // Create final parquet file
        let parquet_path = parquet_dir.join(format!("{subdir_name}.parquet"));
        let mut file = File::create(&parquet_path).map_err(|e| {
            AppError::IoError(format!(
                "Failed to create Parquet file {parquet_path:?}: {e}"
            ))
        })?;

        ParquetWriter::new(&mut file)
            .finish(&mut df)
            .map_err(|e| AppError::ParseError(format!("Failed to write Parquet file: {e}")))?;

        // temp_files Vec is dropped here, automatically deleting all temporary files

        processed_count += 1;
        {
            let pb = progress_bar.lock().unwrap();
            pb.set_message(format!("Completed {subdir_name}"));
        }
    }

    {
        let pb = progress_bar.lock().unwrap();
        pb.finish_with_message(format!("Processed {processed_count} period(s)"));
    }

    info!(
        processed = processed_count,
        skipped = skipped_count,
        "Parsing completed"
    );

    Ok(())
}
