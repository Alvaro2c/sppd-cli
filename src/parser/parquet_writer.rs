use crate::errors::{AppError, AppResult};
use crate::models::{Entry, ProcurementProjectLot};
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

fn lots_to_struct_series(lots: &[ProcurementProjectLot]) -> AppResult<Series> {
    let mut ids = Vec::with_capacity(lots.len());
    let mut names = Vec::with_capacity(lots.len());
    let mut totals = Vec::with_capacity(lots.len());
    let mut total_currencies = Vec::with_capacity(lots.len());
    let mut tax_exclusives = Vec::with_capacity(lots.len());
    let mut tax_currencies = Vec::with_capacity(lots.len());
    let mut cpvs = Vec::with_capacity(lots.len());
    let mut cpv_list_uris = Vec::with_capacity(lots.len());
    let mut countries = Vec::with_capacity(lots.len());
    let mut country_list_uris = Vec::with_capacity(lots.len());

    for lot in lots {
        ids.push(lot.id.clone());
        names.push(lot.name.clone());
        totals.push(lot.total_amount.clone());
        total_currencies.push(lot.total_currency.clone());
        tax_exclusives.push(lot.tax_exclusive_amount.clone());
        tax_currencies.push(lot.tax_exclusive_currency.clone());
        cpvs.push(lot.cpv_code.clone());
        cpv_list_uris.push(lot.cpv_code_list_uri.clone());
        countries.push(lot.country_code.clone());
        country_list_uris.push(lot.country_code_list_uri.clone());
    }

    let df = DataFrame::new(vec![
        Series::new("id", ids),
        Series::new("name", names),
        Series::new("total_amount", totals),
        Series::new("total_currency", total_currencies),
        Series::new("tax_exclusive_amount", tax_exclusives),
        Series::new("tax_exclusive_currency", tax_currencies),
        Series::new("cpv_code", cpvs),
        Series::new("cpv_code_list_uri", cpv_list_uris),
        Series::new("country_code", countries),
        Series::new("country_code_list_uri", country_list_uris),
    ])
    .map_err(|e| AppError::ParseError(format!("Failed to build lot struct: {e}")))?;

    Ok(df.into_struct("lot").into_series())
}

/// Converts a vector of Entry structs into a Polars DataFrame.
///
/// This helper function creates a DataFrame from a slice of Entry structs,
/// ensuring consistent schema across all DataFrame creations.
/// Optimized to pre-allocate vectors and use take() instead of clone() where possible.
fn entries_to_dataframe(entries: Vec<Entry>) -> AppResult<DataFrame> {
    let empty: Vec<Option<String>> = Vec::new();
    if entries.is_empty() {
        let empty_list = Series::new("project_lots", Vec::<Series>::new());

        return DataFrame::new(vec![
            Series::new("id", empty.clone()),
            Series::new("title", empty.clone()),
            Series::new("link", empty.clone()),
            Series::new("summary", empty.clone()),
            Series::new("updated", empty.clone()),
            Series::new("status_code", empty.clone()),
            Series::new("status_code_list_uri", empty.clone()),
            Series::new("contract_id", empty.clone()),
            Series::new("contracting_party_name", empty.clone()),
            Series::new("contracting_party_website", empty.clone()),
            Series::new("contracting_party_type_code", empty.clone()),
            Series::new("contracting_party_type_code_list_uri", empty.clone()),
            Series::new("contracting_party_activity_code", empty.clone()),
            Series::new("contracting_party_activity_code_list_uri", empty.clone()),
            Series::new("contracting_party_city", empty.clone()),
            Series::new("contracting_party_zip", empty.clone()),
            Series::new("contracting_party_country_code", empty.clone()),
            Series::new("contracting_party_country_code_list_uri", empty.clone()),
            Series::new("project_name", empty.clone()),
            Series::new("project_type_code", empty.clone()),
            Series::new("project_type_code_list_uri", empty.clone()),
            Series::new("project_sub_type_code", empty.clone()),
            Series::new("project_sub_type_code_list_uri", empty.clone()),
            Series::new("project_total_amount", empty.clone()),
            Series::new("project_total_currency", empty.clone()),
            Series::new("project_tax_exclusive_amount", empty.clone()),
            Series::new("project_tax_exclusive_currency", empty.clone()),
            Series::new("project_cpv_code", empty.clone()),
            Series::new("project_cpv_code_list_uri", empty.clone()),
            Series::new("project_country_code", empty.clone()),
            Series::new("project_country_code_list_uri", empty.clone()),
            empty_list,
            Series::new("result_code", empty.clone()),
            Series::new("result_code_list_uri", empty.clone()),
            Series::new("result_description", empty.clone()),
            Series::new("result_winning_party", empty.clone()),
            Series::new("result_sme_awarded_indicator", empty.clone()),
            Series::new("result_award_date", empty.clone()),
            Series::new("result_tax_exclusive_amount", empty.clone()),
            Series::new("result_tax_exclusive_currency", empty.clone()),
            Series::new("result_payable_amount", empty.clone()),
            Series::new("result_payable_currency", empty.clone()),
            Series::new("terms_funding_program_code", empty.clone()),
            Series::new("terms_funding_program_code_list_uri", empty.clone()),
            Series::new("terms_award_criteria_type_code", empty.clone()),
            Series::new("terms_award_criteria_type_code_list_uri", empty.clone()),
            Series::new("process_end_date", empty.clone()),
            Series::new("process_procedure_code", empty.clone()),
            Series::new("process_procedure_code_list_uri", empty.clone()),
            Series::new("process_urgency_code", empty.clone()),
            Series::new("process_urgency_code_list_uri", empty.clone()),
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
    let mut status_codes = Vec::with_capacity(len);
    let mut status_code_list_uris = Vec::with_capacity(len);
    let mut contract_ids = Vec::with_capacity(len);
    let mut contracting_party_names = Vec::with_capacity(len);
    let mut contracting_party_websites = Vec::with_capacity(len);
    let mut contracting_party_type_codes = Vec::with_capacity(len);
    let mut contracting_party_type_code_list_uris = Vec::with_capacity(len);
    let mut contracting_party_activity_codes = Vec::with_capacity(len);
    let mut contracting_party_activity_code_list_uris = Vec::with_capacity(len);
    let mut contracting_party_cities = Vec::with_capacity(len);
    let mut contracting_party_zips = Vec::with_capacity(len);
    let mut contracting_party_country_codes = Vec::with_capacity(len);
    let mut contracting_party_country_code_list_uris = Vec::with_capacity(len);
    let mut project_names = Vec::with_capacity(len);
    let mut project_type_codes = Vec::with_capacity(len);
    let mut project_type_code_list_uris = Vec::with_capacity(len);
    let mut project_sub_type_codes = Vec::with_capacity(len);
    let mut project_sub_type_code_list_uris = Vec::with_capacity(len);
    let mut project_total_amounts = Vec::with_capacity(len);
    let mut project_total_currencies = Vec::with_capacity(len);
    let mut project_tax_exclusive_amounts = Vec::with_capacity(len);
    let mut project_tax_exclusive_currencies = Vec::with_capacity(len);
    let mut project_cpv_code = Vec::with_capacity(len);
    let mut project_cpv_code_list_uris = Vec::with_capacity(len);
    let mut project_country_codes = Vec::with_capacity(len);
    let mut project_country_code_list_uris = Vec::with_capacity(len);
    let mut project_lots_structs: Vec<Series> = Vec::with_capacity(len);
    let mut result_codes = Vec::with_capacity(len);
    let mut result_code_list_uris = Vec::with_capacity(len);
    let mut result_descriptions = Vec::with_capacity(len);
    let mut result_winning_parties = Vec::with_capacity(len);
    let mut result_sme_awarded_indicators = Vec::with_capacity(len);
    let mut result_award_dates = Vec::with_capacity(len);
    let mut result_tax_exclusive_amounts = Vec::with_capacity(len);
    let mut result_tax_exclusive_currencies = Vec::with_capacity(len);
    let mut result_payable_amounts = Vec::with_capacity(len);
    let mut result_payable_currencies = Vec::with_capacity(len);
    let mut terms_funding_program_codes = Vec::with_capacity(len);
    let mut terms_funding_program_code_list_uris = Vec::with_capacity(len);
    let mut terms_award_criteria_type_codes = Vec::with_capacity(len);
    let mut terms_award_criteria_type_code_list_uris = Vec::with_capacity(len);
    let mut process_end_dates = Vec::with_capacity(len);
    let mut process_procedure_codes = Vec::with_capacity(len);
    let mut process_procedure_code_list_uris = Vec::with_capacity(len);
    let mut process_urgency_codes = Vec::with_capacity(len);
    let mut process_urgency_code_list_uris = Vec::with_capacity(len);
    let mut cfs_raw_xmls = Vec::with_capacity(len);

    for entry in entries {
        ids.push(entry.id);
        titles.push(entry.title);
        links.push(entry.link);
        summaries.push(entry.summary);
        updateds.push(entry.updated);
        status_codes.push(entry.status_code);
        status_code_list_uris.push(entry.status_code_list_uri);
        contract_ids.push(entry.contract_id);
        contracting_party_names.push(entry.contracting_party_name);
        contracting_party_websites.push(entry.contracting_party_website);
        contracting_party_type_codes.push(entry.contracting_party_type_code);
        contracting_party_type_code_list_uris.push(entry.contracting_party_type_code_list_uri);
        contracting_party_activity_codes.push(entry.contracting_party_activity_code);
        contracting_party_activity_code_list_uris
            .push(entry.contracting_party_activity_code_list_uri);
        contracting_party_cities.push(entry.contracting_party_city);
        contracting_party_zips.push(entry.contracting_party_zip);
        contracting_party_country_codes.push(entry.contracting_party_country_code);
        contracting_party_country_code_list_uris
            .push(entry.contracting_party_country_code_list_uri);
        project_names.push(entry.project_name);
        project_type_codes.push(entry.project_type_code);
        project_type_code_list_uris.push(entry.project_type_code_list_uri);
        project_sub_type_codes.push(entry.project_sub_type_code);
        project_sub_type_code_list_uris.push(entry.project_sub_type_code_list_uri);
        project_total_amounts.push(entry.project_total_amount);
        project_total_currencies.push(entry.project_total_currency);
        project_tax_exclusive_amounts.push(entry.project_tax_exclusive_amount);
        project_tax_exclusive_currencies.push(entry.project_tax_exclusive_currency);
        project_cpv_code.push(entry.project_cpv_code);
        project_cpv_code_list_uris.push(entry.project_cpv_code_list_uri);
        project_country_codes.push(entry.project_country_code);
        project_country_code_list_uris.push(entry.project_country_code_list_uri);
        let lot_struct = lots_to_struct_series(&entry.project_lots)?;
        project_lots_structs.push(lot_struct);
        result_codes.push(entry.result_code);
        result_code_list_uris.push(entry.result_code_list_uri);
        result_descriptions.push(entry.result_description);
        result_winning_parties.push(entry.result_winning_party);
        result_sme_awarded_indicators.push(entry.result_sme_awarded_indicator);
        result_award_dates.push(entry.result_award_date);
        result_tax_exclusive_amounts.push(entry.result_tax_exclusive_amount);
        result_tax_exclusive_currencies.push(entry.result_tax_exclusive_currency);
        result_payable_amounts.push(entry.result_payable_amount);
        result_payable_currencies.push(entry.result_payable_currency);
        terms_funding_program_codes.push(entry.terms_funding_program_code);
        terms_funding_program_code_list_uris.push(entry.terms_funding_program_code_list_uri);
        terms_award_criteria_type_codes.push(entry.terms_award_criteria_type_code);
        terms_award_criteria_type_code_list_uris
            .push(entry.terms_award_criteria_type_code_list_uri);
        process_end_dates.push(entry.process_end_date);
        process_procedure_codes.push(entry.process_procedure_code);
        process_procedure_code_list_uris.push(entry.process_procedure_code_list_uri);
        process_urgency_codes.push(entry.process_urgency_code);
        process_urgency_code_list_uris.push(entry.process_urgency_code_list_uri);
        cfs_raw_xmls.push(entry.cfs_raw_xml);
    }

    let project_lots_series = Series::new("project_lots", project_lots_structs);

    DataFrame::new(vec![
        Series::new("id", ids),
        Series::new("title", titles),
        Series::new("link", links),
        Series::new("summary", summaries),
        Series::new("updated", updateds),
        Series::new("status_code", status_codes),
        Series::new("status_code_list_uri", status_code_list_uris),
        Series::new("contract_id", contract_ids),
        Series::new("contracting_party_name", contracting_party_names),
        Series::new("contracting_party_website", contracting_party_websites),
        Series::new("contracting_party_type_code", contracting_party_type_codes),
        Series::new(
            "contracting_party_type_code_list_uri",
            contracting_party_type_code_list_uris,
        ),
        Series::new(
            "contracting_party_activity_code",
            contracting_party_activity_codes,
        ),
        Series::new(
            "contracting_party_activity_code_list_uri",
            contracting_party_activity_code_list_uris,
        ),
        Series::new("contracting_party_city", contracting_party_cities),
        Series::new("contracting_party_zip", contracting_party_zips),
        Series::new(
            "contracting_party_country_code",
            contracting_party_country_codes,
        ),
        Series::new(
            "contracting_party_country_code_list_uri",
            contracting_party_country_code_list_uris,
        ),
        Series::new("project_name", project_names),
        Series::new("project_type_code", project_type_codes),
        Series::new("project_type_code_list_uri", project_type_code_list_uris),
        Series::new("project_sub_type_code", project_sub_type_codes),
        Series::new(
            "project_sub_type_code_list_uri",
            project_sub_type_code_list_uris,
        ),
        Series::new("project_total_amount", project_total_amounts),
        Series::new("project_total_currency", project_total_currencies),
        Series::new(
            "project_tax_exclusive_amount",
            project_tax_exclusive_amounts,
        ),
        Series::new(
            "project_tax_exclusive_currency",
            project_tax_exclusive_currencies,
        ),
        Series::new("project_cpv_code", project_cpv_code),
        Series::new("project_cpv_code_list_uri", project_cpv_code_list_uris),
        Series::new("project_country_code", project_country_codes),
        Series::new(
            "project_country_code_list_uri",
            project_country_code_list_uris,
        ),
        project_lots_series,
        Series::new("result_code", result_codes),
        Series::new("result_code_list_uri", result_code_list_uris),
        Series::new("result_description", result_descriptions),
        Series::new("result_winning_party", result_winning_parties),
        Series::new(
            "result_sme_awarded_indicator",
            result_sme_awarded_indicators,
        ),
        Series::new("result_award_date", result_award_dates),
        Series::new("result_tax_exclusive_amount", result_tax_exclusive_amounts),
        Series::new(
            "result_tax_exclusive_currency",
            result_tax_exclusive_currencies,
        ),
        Series::new("result_payable_amount", result_payable_amounts),
        Series::new("result_payable_currency", result_payable_currencies),
        Series::new("terms_funding_program_code", terms_funding_program_codes),
        Series::new(
            "terms_funding_program_code_list_uri",
            terms_funding_program_code_list_uris,
        ),
        Series::new(
            "terms_award_criteria_type_code",
            terms_award_criteria_type_codes,
        ),
        Series::new(
            "terms_award_criteria_type_code_list_uri",
            terms_award_criteria_type_code_list_uris,
        ),
        Series::new("process_end_date", process_end_dates),
        Series::new("process_procedure_code", process_procedure_codes),
        Series::new(
            "process_procedure_code_list_uri",
            process_procedure_code_list_uris,
        ),
        Series::new("process_urgency_code", process_urgency_codes),
        Series::new(
            "process_urgency_code_list_uri",
            process_urgency_code_list_uris,
        ),
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
        assert_eq!(df.width(), 60);
    }

    #[test]
    fn entries_to_dataframe_single_entry() {
        let entry = Entry {
            id: Some("id".to_string()),
            title: Some("title".to_string()),
            link: Some("link".to_string()),
            summary: Some("summary".to_string()),
            updated: Some("2023-01-01".to_string()),
            status_code: None,
            status_code_list_uri: None,
            contract_id: None,
            contracting_party_name: None,
            contracting_party_website: None,
            contracting_party_type_code: None,
            contracting_party_type_code_list_uri: None,
            contracting_party_activity_code: None,
            contracting_party_activity_code_list_uri: None,
            contracting_party_city: None,
            contracting_party_zip: None,
            contracting_party_country_code: None,
            contracting_party_country_code_list_uri: None,
            project_name: None,
            project_type_code: None,
            project_type_code_list_uri: None,
            project_sub_type_code: None,
            project_sub_type_code_list_uri: None,
            project_total_amount: None,
            project_total_currency: None,
            project_tax_exclusive_amount: None,
            project_tax_exclusive_currency: None,
            project_cpv_code: None,
            project_cpv_code_list_uri: None,
            project_country_code: None,
            project_country_code_list_uri: None,
            project_lots: Vec::new(),
            result_code: None,
            result_code_list_uri: None,
            result_description: None,
            result_winning_party: None,
            result_sme_awarded_indicator: None,
            result_award_date: None,
            result_tax_exclusive_amount: None,
            result_tax_exclusive_currency: None,
            result_payable_amount: None,
            result_payable_currency: None,
            terms_funding_program_code: None,
            terms_funding_program_code_list_uri: None,
            terms_award_criteria_type_code: None,
            terms_award_criteria_type_code_list_uri: None,
            process_end_date: None,
            process_procedure_code: None,
            process_procedure_code_list_uri: None,
            process_urgency_code: None,
            process_urgency_code_list_uri: None,
            cfs_raw_xml: Some("<xml/>".to_string()),
        };

        let df = entries_to_dataframe(vec![entry]).unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 52);
        let lots_col = df.column("project_lots").unwrap();
        assert!(matches!(lots_col.dtype(), DataType::List(_)));
        let value = df.column("id").unwrap().get(0).unwrap();
        assert_eq!(value, AnyValue::String("id"));
    }
}
