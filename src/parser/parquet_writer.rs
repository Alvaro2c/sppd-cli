use crate::errors::{AppError, AppResult};
use crate::models::Entry;
use crate::utils::{format_duration, mb_from_bytes, round_two_decimals};
use futures::stream::{self, StreamExt, TryStreamExt};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::fs::{self as std_fs, File};
use std::path::PathBuf;
use std::time::Instant;
use tokio::fs as tokio_fs;
use tracing::info;

use super::file_finder::find_xmls;
use super::xml_parser::parse_xml_bytes;

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

async fn read_xml_contents(paths: &[PathBuf]) -> AppResult<Vec<Vec<u8>>> {
    const READ_CONCURRENCY: usize = 16;
    stream::iter(paths.iter().cloned())
        .map(|path| async move {
            tokio_fs::read(&path)
                .await
                .map_err(|e| AppError::IoError(format!("Failed to read XML file {path:?}: {e}")))
        })
        .buffered(READ_CONCURRENCY)
        .try_collect()
        .await
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
/// * `batch_size` - Number of XML files to process per chunk (affects memory usage)
/// * `config` - Resolved configuration containing directory paths
///
/// # Behavior
///
/// - **Filtering**: Only processes subdirectories whose names match keys in `target_links`
/// - **Skip empty**: Subdirectories with no entries are skipped (logged but not an error)
/// - **Progress tracking**: Elapsed time and throughput are logged after parsing completes
///
/// # Errors
///
/// Returns an error if:
/// - Directory creation fails
/// - XML parsing fails
/// - DataFrame creation fails
/// - Parquet file writing fails
pub async fn parse_xmls(
    target_links: &BTreeMap<String, String>,
    procurement_type: &crate::models::ProcurementType,
    batch_size: usize,
    config: &crate::config::ResolvedConfig,
) -> AppResult<()> {
    let extract_dir = procurement_type.extract_dir(config);
    let parquet_dir = procurement_type.parquet_dir(config);

    // Create parquet directory if it doesn't exist
    std_fs::create_dir_all(&parquet_dir)
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

    // Calculate total XML files across all periods for logging
    let total_xml_files: usize = subdirs_to_process
        .iter()
        .map(|(_, files)| files.len())
        .sum();

    let start = Instant::now();
    let mut total_parquet_bytes = 0u64;

    info!(total = total_subdirs, "Starting XML parsing");

    let mut processed_count = 0;
    let mut skipped_count = 0;

    // Process each subdirectory
    for (subdir_name, xml_files) in subdirs_to_process {
        let chunk_size = batch_size.max(1);
        let mut df: Option<DataFrame> = None;
        let mut has_entries = false;

        for xml_chunk in xml_files.chunks(chunk_size) {
            let xml_contents = read_xml_contents(xml_chunk).await?;
            let parsed_entry_batches: Vec<Vec<Entry>> = xml_contents
                .par_iter()
                .map(|content| parse_xml_bytes(content))
                .collect::<AppResult<Vec<_>>>()?;

            let mut chunk_entries = Vec::new();
            for mut entries in parsed_entry_batches {
                if entries.is_empty() {
                    continue;
                }
                chunk_entries.append(&mut entries);
            }

            if chunk_entries.is_empty() {
                continue;
            }

            has_entries = true;
            let chunk_df = entries_to_dataframe(chunk_entries)?;
            df = Some(match df {
                Some(existing) => existing.vstack(&chunk_df).map_err(|e| {
                    AppError::ParseError(format!("Failed to concatenate DataFrames: {e}"))
                })?,
                None => chunk_df,
            });
        }

        if !has_entries {
            skipped_count += 1;
            continue;
        }

        let mut df = df.unwrap();

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

        processed_count += 1;
        let metadata = std_fs::metadata(&parquet_path).map_err(|e| {
            AppError::IoError(format!(
                "Failed to read Parquet file metadata {parquet_path:?}: {e}"
            ))
        })?;
        total_parquet_bytes += metadata.len();
    }

    let elapsed = start.elapsed();
    let elapsed_str = format_duration(elapsed);
    let total_mb = mb_from_bytes(total_parquet_bytes);
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        total_mb / elapsed.as_secs_f64()
    } else {
        total_mb
    };
    let size_mb = round_two_decimals(total_mb);
    let throughput_mb_s = round_two_decimals(throughput);

    info!(
        processed = processed_count,
        skipped = skipped_count,
        xml_files = total_xml_files,
        parquet_files = processed_count,
        elapsed = elapsed_str,
        output_size_mb = size_mb,
        throughput_mb_s = throughput_mb_s,
        "Parsing completed"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_to_dataframe_empty_yields_zero_rows() {
        let df = entries_to_dataframe(vec![]).unwrap();
        assert_eq!(df.height(), 0);
        assert_eq!(df.width(), 6);
    }

    #[test]
    fn entries_to_dataframe_single_entry() {
        let entry = Entry {
            id: Some("id".to_string()),
            title: Some("title".to_string()),
            link: Some("link".to_string()),
            summary: Some("summary".to_string()),
            updated: Some("2023-01-01".to_string()),
            contract_folder_status: Some("{}".to_string()),
        };

        let df = entries_to_dataframe(vec![entry]).unwrap();
        assert_eq!(df.height(), 1);
        let value = df.column("id").unwrap().get(0).unwrap();
        assert_eq!(value, AnyValue::String("id"));
    }
}
