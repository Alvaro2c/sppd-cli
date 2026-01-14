use crate::errors::{AppError, AppResult};
use crate::models::Entry;
use crate::utils::{format_duration, mb_from_bytes, round_two_decimals};
use futures::stream::{self, StreamExt, TryStreamExt};
use polars::lazy::prelude::{LazyFrame, ScanArgsParquet};
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
    let empty: Vec<Option<String>> = Vec::new();
    if entries.is_empty() {
        return DataFrame::new(vec![
            Series::new("id", empty.clone()),
            Series::new("title", empty.clone()),
            Series::new("link", empty.clone()),
            Series::new("summary", empty.clone()),
            Series::new("updated", empty.clone()),
            Series::new("cfs_status_code", empty.clone()),
            Series::new("cfs_id", empty.clone()),
            Series::new("cfs_project_name", empty.clone()),
            Series::new("cfs_project_type_code", empty.clone()),
            Series::new("cfs_project_total_amount", empty.clone()),
            Series::new("cfs_project_total_currency", empty.clone()),
            Series::new("cfs_project_tax_exclusive_amount", empty.clone()),
            Series::new("cfs_project_tax_exclusive_currency", empty.clone()),
            Series::new("cfs_project_cpv_codes", empty.clone()),
            Series::new("cfs_project_country_code", empty.clone()),
            Series::new("cfs_project_lot_name", empty.clone()),
            Series::new("cfs_project_lot_type_code", empty.clone()),
            Series::new("cfs_project_lot_total_amount", empty.clone()),
            Series::new("cfs_project_lot_total_currency", empty.clone()),
            Series::new("cfs_project_lot_tax_exclusive_amount", empty.clone()),
            Series::new("cfs_project_lot_tax_exclusive_currency", empty.clone()),
            Series::new("cfs_project_lot_cpv_codes", empty.clone()),
            Series::new("cfs_project_lot_country_code", empty.clone()),
            Series::new("cfs_contracting_party_name", empty.clone()),
            Series::new("cfs_contracting_party_website", empty.clone()),
            Series::new("cfs_contracting_party_type_code", empty.clone()),
            Series::new("cfs_result_code", empty.clone()),
            Series::new("cfs_result_description", empty.clone()),
            Series::new("cfs_result_winning_party", empty.clone()),
            Series::new("cfs_result_tax_exclusive_amount", empty.clone()),
            Series::new("cfs_result_tax_exclusive_currency", empty.clone()),
            Series::new("cfs_result_payable_amount", empty.clone()),
            Series::new("cfs_result_payable_currency", empty.clone()),
            Series::new("cfs_process_procedure_code", empty.clone()),
            Series::new("cfs_process_urgency_code", empty.clone()),
            Series::new("cfs_raw_xml", empty),
        ])
        .map_err(|e| AppError::ParseError(format!("Failed to create DataFrame: {e}")));
    }

    let len = entries.len();
    let mut ids = Vec::with_capacity(len);
    let mut titles = Vec::with_capacity(len);
    let mut links = Vec::with_capacity(len);
    let mut summaries = Vec::with_capacity(len);
    let mut updateds = Vec::with_capacity(len);
    let mut cfs_status_codes = Vec::with_capacity(len);
    let mut cfs_ids = Vec::with_capacity(len);
    let mut cfs_project_names = Vec::with_capacity(len);
    let mut cfs_project_type_codes = Vec::with_capacity(len);
    let mut cfs_project_total_amounts = Vec::with_capacity(len);
    let mut cfs_project_total_currencies = Vec::with_capacity(len);
    let mut cfs_project_tax_exclusive_amounts = Vec::with_capacity(len);
    let mut cfs_project_tax_exclusive_currencies = Vec::with_capacity(len);
    let mut cfs_project_cpv_codes_vec = Vec::with_capacity(len);
    let mut cfs_project_country_codes = Vec::with_capacity(len);
    let mut cfs_project_lot_names = Vec::with_capacity(len);
    let mut cfs_project_lot_type_codes = Vec::with_capacity(len);
    let mut cfs_project_lot_total_amounts = Vec::with_capacity(len);
    let mut cfs_project_lot_total_currencies = Vec::with_capacity(len);
    let mut cfs_project_lot_tax_exclusive_amounts = Vec::with_capacity(len);
    let mut cfs_project_lot_tax_exclusive_currencies = Vec::with_capacity(len);
    let mut cfs_project_lot_cpv_codes_vec = Vec::with_capacity(len);
    let mut cfs_project_lot_country_codes = Vec::with_capacity(len);
    let mut cfs_contracting_party_names = Vec::with_capacity(len);
    let mut cfs_contracting_party_websites = Vec::with_capacity(len);
    let mut cfs_contracting_party_type_codes = Vec::with_capacity(len);
    let mut cfs_result_codes = Vec::with_capacity(len);
    let mut cfs_result_descriptions = Vec::with_capacity(len);
    let mut cfs_result_winning_parties = Vec::with_capacity(len);
    let mut cfs_result_tax_exclusive_amounts = Vec::with_capacity(len);
    let mut cfs_result_tax_exclusive_currencies = Vec::with_capacity(len);
    let mut cfs_result_payable_amounts = Vec::with_capacity(len);
    let mut cfs_result_payable_currencies = Vec::with_capacity(len);
    let mut cfs_process_procedure_codes = Vec::with_capacity(len);
    let mut cfs_process_urgency_codes = Vec::with_capacity(len);
    let mut cfs_raw_xmls = Vec::with_capacity(len);

    for entry in entries {
        ids.push(entry.id);
        titles.push(entry.title);
        links.push(entry.link);
        summaries.push(entry.summary);
        updateds.push(entry.updated);
        cfs_status_codes.push(entry.cfs_status_code);
        cfs_ids.push(entry.cfs_id);
        cfs_project_names.push(entry.cfs_project_name);
        cfs_project_type_codes.push(entry.cfs_project_type_code);
        cfs_project_total_amounts.push(entry.cfs_project_total_amount);
        cfs_project_total_currencies.push(entry.cfs_project_total_currency);
        cfs_project_tax_exclusive_amounts.push(entry.cfs_project_tax_exclusive_amount);
        cfs_project_tax_exclusive_currencies.push(entry.cfs_project_tax_exclusive_currency);
        cfs_project_cpv_codes_vec.push(entry.cfs_project_cpv_codes);
        cfs_project_country_codes.push(entry.cfs_project_country_code);
        cfs_project_lot_names.push(entry.cfs_project_lot_name);
        cfs_project_lot_type_codes.push(entry.cfs_project_lot_type_code);
        cfs_project_lot_total_amounts.push(entry.cfs_project_lot_total_amount);
        cfs_project_lot_total_currencies.push(entry.cfs_project_lot_total_currency);
        cfs_project_lot_tax_exclusive_amounts.push(entry.cfs_project_lot_tax_exclusive_amount);
        cfs_project_lot_tax_exclusive_currencies.push(entry.cfs_project_lot_tax_exclusive_currency);
        cfs_project_lot_cpv_codes_vec.push(entry.cfs_project_lot_cpv_codes);
        cfs_project_lot_country_codes.push(entry.cfs_project_lot_country_code);
        cfs_contracting_party_names.push(entry.cfs_contracting_party_name);
        cfs_contracting_party_websites.push(entry.cfs_contracting_party_website);
        cfs_contracting_party_type_codes.push(entry.cfs_contracting_party_type_code);
        cfs_result_codes.push(entry.cfs_result_code);
        cfs_result_descriptions.push(entry.cfs_result_description);
        cfs_result_winning_parties.push(entry.cfs_result_winning_party);
        cfs_result_tax_exclusive_amounts.push(entry.cfs_result_tax_exclusive_amount);
        cfs_result_tax_exclusive_currencies.push(entry.cfs_result_tax_exclusive_currency);
        cfs_result_payable_amounts.push(entry.cfs_result_payable_amount);
        cfs_result_payable_currencies.push(entry.cfs_result_payable_currency);
        cfs_process_procedure_codes.push(entry.cfs_process_procedure_code);
        cfs_process_urgency_codes.push(entry.cfs_process_urgency_code);
        cfs_raw_xmls.push(entry.cfs_raw_xml);
    }

    DataFrame::new(vec![
        Series::new("id", ids),
        Series::new("title", titles),
        Series::new("link", links),
        Series::new("summary", summaries),
        Series::new("updated", updateds),
        Series::new("cfs_status_code", cfs_status_codes),
        Series::new("cfs_id", cfs_ids),
        Series::new("cfs_project_name", cfs_project_names),
        Series::new("cfs_project_type_code", cfs_project_type_codes),
        Series::new("cfs_project_total_amount", cfs_project_total_amounts),
        Series::new("cfs_project_total_currency", cfs_project_total_currencies),
        Series::new(
            "cfs_project_tax_exclusive_amount",
            cfs_project_tax_exclusive_amounts,
        ),
        Series::new(
            "cfs_project_tax_exclusive_currency",
            cfs_project_tax_exclusive_currencies,
        ),
        Series::new("cfs_project_cpv_codes", cfs_project_cpv_codes_vec),
        Series::new("cfs_project_country_code", cfs_project_country_codes),
        Series::new("cfs_project_lot_name", cfs_project_lot_names),
        Series::new("cfs_project_lot_type_code", cfs_project_lot_type_codes),
        Series::new(
            "cfs_project_lot_total_amount",
            cfs_project_lot_total_amounts,
        ),
        Series::new(
            "cfs_project_lot_total_currency",
            cfs_project_lot_total_currencies,
        ),
        Series::new(
            "cfs_project_lot_tax_exclusive_amount",
            cfs_project_lot_tax_exclusive_amounts,
        ),
        Series::new(
            "cfs_project_lot_tax_exclusive_currency",
            cfs_project_lot_tax_exclusive_currencies,
        ),
        Series::new("cfs_project_lot_cpv_codes", cfs_project_lot_cpv_codes_vec),
        Series::new(
            "cfs_project_lot_country_code",
            cfs_project_lot_country_codes,
        ),
        Series::new("cfs_contracting_party_name", cfs_contracting_party_names),
        Series::new(
            "cfs_contracting_party_website",
            cfs_contracting_party_websites,
        ),
        Series::new(
            "cfs_contracting_party_type_code",
            cfs_contracting_party_type_codes,
        ),
        Series::new("cfs_result_code", cfs_result_codes),
        Series::new("cfs_result_description", cfs_result_descriptions),
        Series::new("cfs_result_winning_party", cfs_result_winning_parties),
        Series::new(
            "cfs_result_tax_exclusive_amount",
            cfs_result_tax_exclusive_amounts,
        ),
        Series::new(
            "cfs_result_tax_exclusive_currency",
            cfs_result_tax_exclusive_currencies,
        ),
        Series::new("cfs_result_payable_amount", cfs_result_payable_amounts),
        Series::new("cfs_result_payable_currency", cfs_result_payable_currencies),
        Series::new("cfs_process_procedure_code", cfs_process_procedure_codes),
        Series::new("cfs_process_urgency_code", cfs_process_urgency_codes),
        Series::new("cfs_raw_xml", cfs_raw_xmls),
    ])
    .map_err(|e| AppError::ParseError(format!("Failed to create DataFrame: {e}")))
}

async fn read_xml_contents(paths: &[PathBuf], concurrency: usize) -> AppResult<Vec<Vec<u8>>> {
    let read_concurrency = concurrency.max(1);
    stream::iter(paths.iter().cloned())
        .map(|path| async move {
            tokio_fs::read(&path)
                .await
                .map_err(|e| AppError::IoError(format!("Failed to read XML file {path:?}: {e}")))
        })
        .buffered(read_concurrency)
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
/// 3. Parses XML/Atom files in each matching subdirectory in batches, bounded by `batch_size`
/// 4. Writes each batch to Parquet and optionally concatenates the batches per period
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
/// - **Batch output**: Each chunk results in `data/parquet/{period}/batch_<n>.parquet`, `concat_batches` merges them afterwards
/// - **Memory controls**: `batch_size` bounds the in-flight DataFrame and `read_concurrency` limits parallel reads
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
        let mut has_entries = false;
        let mut batch_index = 0;
        let period_dir = parquet_dir.join(&subdir_name);
        let mut period_dir_created = false;
        let mut batch_paths: Vec<PathBuf> = Vec::new();

        for xml_chunk in xml_files.chunks(chunk_size) {
            let xml_contents = read_xml_contents(xml_chunk, config.read_concurrency).await?;
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

            if !period_dir_created {
                if period_dir.exists() {
                    std_fs::remove_dir_all(&period_dir).map_err(|e| {
                        AppError::IoError(format!(
                            "Failed to remove previous parquet directory {period_dir:?}: {e}"
                        ))
                    })?;
                }
                std_fs::create_dir_all(&period_dir).map_err(|e| {
                    AppError::IoError(format!(
                        "Failed to create parquet period directory {period_dir:?}: {e}"
                    ))
                })?;
                period_dir_created = true;
            }

            has_entries = true;
            let mut chunk_df = entries_to_dataframe(chunk_entries)?;
            let batch_path = period_dir.join(format!("batch_{batch_index}.parquet"));
            let mut file = File::create(&batch_path).map_err(|e| {
                AppError::IoError(format!(
                    "Failed to create Parquet batch file {batch_path:?}: {e}"
                ))
            })?;

            ParquetWriter::new(&mut file)
                .finish(&mut chunk_df)
                .map_err(|e| AppError::ParseError(format!("Failed to write Parquet batch: {e}")))?;

            batch_paths.push(batch_path);
            batch_index += 1;
        }

        if !has_entries {
            skipped_count += 1;
            if period_dir_created {
                std_fs::remove_dir_all(&period_dir).map_err(|e| {
                    AppError::IoError(format!(
                        "Failed to remove empty parquet directory {period_dir:?}: {e}"
                    ))
                })?;
            }
            continue;
        }

        let mut output_paths = Vec::new();
        if config.concat_batches {
            let glob_path = period_dir.join("batch_*.parquet");
            let glob_str = glob_path.to_string_lossy().into_owned();
            let mut combined = LazyFrame::scan_parquet(&glob_str, ScanArgsParquet::default())
                .map_err(|e| {
                    AppError::ParseError(format!(
                        "Failed to scan parquet batches for {subdir_name}: {e}"
                    ))
                })?
                .collect()
                .map_err(|e| {
                    AppError::ParseError(format!(
                        "Failed to collect combined DataFrame for {subdir_name}: {e}"
                    ))
                })?;

            let final_path = parquet_dir.join(format!("{subdir_name}.parquet"));
            let mut final_file = File::create(&final_path).map_err(|e| {
                AppError::IoError(format!(
                    "Failed to create final Parquet file {final_path:?}: {e}"
                ))
            })?;

            ParquetWriter::new(&mut final_file)
                .finish(&mut combined)
                .map_err(|e| {
                    AppError::ParseError(format!("Failed to write final Parquet file: {e}"))
                })?;

            output_paths.push(final_path);
            std_fs::remove_dir_all(&period_dir).map_err(|e| {
                AppError::IoError(format!(
                    "Failed to remove temporary parquet directory {period_dir:?}: {e}"
                ))
            })?;
        } else {
            output_paths.extend(batch_paths.iter().cloned());
        }

        for output_path in output_paths {
            let metadata = std_fs::metadata(&output_path).map_err(|e| {
                AppError::IoError(format!(
                    "Failed to read Parquet file metadata {output_path:?}: {e}"
                ))
            })?;
            total_parquet_bytes += metadata.len();
        }

        processed_count += 1;
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
        assert_eq!(df.width(), 36);
    }

    #[test]
    fn entries_to_dataframe_single_entry() {
        let entry = Entry {
            id: Some("id".to_string()),
            title: Some("title".to_string()),
            link: Some("link".to_string()),
            summary: Some("summary".to_string()),
            updated: Some("2023-01-01".to_string()),
            cfs_status_code: None,
            cfs_id: None,
            cfs_project_name: None,
            cfs_project_type_code: None,
            cfs_project_total_amount: None,
            cfs_project_total_currency: None,
            cfs_project_tax_exclusive_amount: None,
            cfs_project_tax_exclusive_currency: None,
            cfs_project_cpv_codes: None,
            cfs_project_country_code: None,
            cfs_project_lot_name: None,
            cfs_project_lot_type_code: None,
            cfs_project_lot_total_amount: None,
            cfs_project_lot_total_currency: None,
            cfs_project_lot_tax_exclusive_amount: None,
            cfs_project_lot_tax_exclusive_currency: None,
            cfs_project_lot_cpv_codes: None,
            cfs_project_lot_country_code: None,
            cfs_contracting_party_name: None,
            cfs_contracting_party_website: None,
            cfs_contracting_party_type_code: None,
            cfs_result_code: None,
            cfs_result_description: None,
            cfs_result_winning_party: None,
            cfs_result_tax_exclusive_amount: None,
            cfs_result_tax_exclusive_currency: None,
            cfs_result_payable_amount: None,
            cfs_result_payable_currency: None,
            cfs_process_procedure_code: None,
            cfs_process_urgency_code: None,
            cfs_raw_xml: Some("<xml/>".to_string()),
        };

        let df = entries_to_dataframe(vec![entry]).unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 36);
        let value = df.column("id").unwrap().get(0).unwrap();
        assert_eq!(value, AnyValue::String("id"));
    }
}
