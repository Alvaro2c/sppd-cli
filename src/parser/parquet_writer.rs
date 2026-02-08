use crate::errors::{AppError, AppResult};
use crate::models::{Entry, ProcurementProjectLot, TenderResultRow};
use crate::utils::{format_duration, mb_from_bytes, round_two_decimals};
use futures::stream::{self, StreamExt, TryStreamExt};
use polars::lazy::prelude::{LazyFrame, ScanArgsParquet};
use polars::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::BTreeMap;
use std::fs::{self as std_fs, File};
use std::path::PathBuf;
use std::time::Instant;
use tokio::fs as tokio_fs;
use tracing::{info, warn};

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

fn tender_results_to_struct_series(results: &[TenderResultRow]) -> AppResult<Series> {
    let mut result_ids = Vec::with_capacity(results.len());
    let mut result_lot_ids = Vec::with_capacity(results.len());
    let mut result_codes = Vec::with_capacity(results.len());
    let mut result_code_list_uris = Vec::with_capacity(results.len());
    let mut descriptions = Vec::with_capacity(results.len());
    let mut winning_parties = Vec::with_capacity(results.len());
    let mut sme_indicators = Vec::with_capacity(results.len());
    let mut award_dates = Vec::with_capacity(results.len());
    let mut tax_exclusive_amounts = Vec::with_capacity(results.len());
    let mut tax_exclusive_currencies = Vec::with_capacity(results.len());
    let mut payable_amounts = Vec::with_capacity(results.len());
    let mut payable_currencies = Vec::with_capacity(results.len());

    for result in results {
        result_ids.push(result.result_id.clone());
        result_lot_ids.push(result.result_lot_id.clone());
        result_codes.push(result.result_code.clone());
        result_code_list_uris.push(result.result_code_list_uri.clone());
        descriptions.push(result.result_description.clone());
        winning_parties.push(result.result_winning_party.clone());
        sme_indicators.push(result.result_sme_awarded_indicator.clone());
        award_dates.push(result.result_award_date.clone());
        tax_exclusive_amounts.push(result.result_tax_exclusive_amount.clone());
        tax_exclusive_currencies.push(result.result_tax_exclusive_currency.clone());
        payable_amounts.push(result.result_payable_amount.clone());
        payable_currencies.push(result.result_payable_currency.clone());
    }

    let df = DataFrame::new(vec![
        Series::new("result_id", result_ids),
        Series::new("result_lot_id", result_lot_ids),
        Series::new("result_code", result_codes),
        Series::new("result_code_list_uri", result_code_list_uris),
        Series::new("result_description", descriptions),
        Series::new("result_winning_party", winning_parties),
        Series::new("result_sme_awarded_indicator", sme_indicators),
        Series::new("result_award_date", award_dates),
        Series::new("result_tax_exclusive_amount", tax_exclusive_amounts),
        Series::new("result_tax_exclusive_currency", tax_exclusive_currencies),
        Series::new("result_payable_amount", payable_amounts),
        Series::new("result_payable_currency", payable_currencies),
    ])
    .map_err(|e| AppError::ParseError(format!("Failed to build tender_result struct: {e}")))?;

    Ok(df.into_struct("tender_result").into_series())
}

fn status_to_struct(entries: &[Entry]) -> AppResult<Series> {
    let mut codes = Vec::with_capacity(entries.len());
    let mut list_uris = Vec::with_capacity(entries.len());

    for entry in entries {
        codes.push(entry.status.code.clone());
        list_uris.push(entry.status.list_uri.clone());
    }

    let df = DataFrame::new(vec![
        Series::new("code", codes),
        Series::new("list_uri", list_uris),
    ])
    .map_err(|e| AppError::ParseError(format!("Failed to build status struct: {e}")))?;

    Ok(df.into_struct("status").into_series())
}

fn terms_funding_program_to_struct(entries: &[Entry]) -> AppResult<Series> {
    let mut codes = Vec::with_capacity(entries.len());
    let mut list_uris = Vec::with_capacity(entries.len());

    for entry in entries {
        codes.push(entry.terms_funding_program.code.clone());
        list_uris.push(entry.terms_funding_program.list_uri.clone());
    }

    let df = DataFrame::new(vec![
        Series::new("code", codes),
        Series::new("list_uri", list_uris),
    ])
    .map_err(|e| {
        AppError::ParseError(format!("Failed to build terms_funding_program struct: {e}"))
    })?;

    Ok(df.into_struct("terms_funding_program").into_series())
}

fn contracting_party_to_struct(entries: &[Entry]) -> AppResult<Series> {
    let mut names = Vec::with_capacity(entries.len());
    let mut websites = Vec::with_capacity(entries.len());
    let mut type_codes = Vec::with_capacity(entries.len());
    let mut type_code_list_uris = Vec::with_capacity(entries.len());
    let mut activity_codes = Vec::with_capacity(entries.len());
    let mut activity_code_list_uris = Vec::with_capacity(entries.len());
    let mut cities = Vec::with_capacity(entries.len());
    let mut zips = Vec::with_capacity(entries.len());
    let mut country_codes = Vec::with_capacity(entries.len());
    let mut country_code_list_uris = Vec::with_capacity(entries.len());

    for entry in entries {
        names.push(entry.contracting_party_name.clone());
        websites.push(entry.contracting_party_website.clone());
        type_codes.push(entry.contracting_party_type_code.clone());
        type_code_list_uris.push(entry.contracting_party_type_code_list_uri.clone());
        activity_codes.push(entry.contracting_party_activity_code.clone());
        activity_code_list_uris.push(entry.contracting_party_activity_code_list_uri.clone());
        cities.push(entry.contracting_party_city.clone());
        zips.push(entry.contracting_party_zip.clone());
        country_codes.push(entry.contracting_party_country_code.clone());
        country_code_list_uris.push(entry.contracting_party_country_code_list_uri.clone());
    }

    let df = DataFrame::new(vec![
        Series::new("name", names),
        Series::new("website", websites),
        Series::new("type_code", type_codes),
        Series::new("type_code_list_uri", type_code_list_uris),
        Series::new("activity_code", activity_codes),
        Series::new("activity_code_list_uri", activity_code_list_uris),
        Series::new("city", cities),
        Series::new("zip", zips),
        Series::new("country_code", country_codes),
        Series::new("country_code_list_uri", country_code_list_uris),
    ])
    .map_err(|e| AppError::ParseError(format!("Failed to build contracting_party struct: {e}")))?;

    Ok(df.into_struct("contracting_party").into_series())
}

fn project_to_struct(entries: &[Entry]) -> AppResult<Series> {
    let mut names = Vec::with_capacity(entries.len());
    let mut type_codes = Vec::with_capacity(entries.len());
    let mut type_code_list_uris = Vec::with_capacity(entries.len());
    let mut sub_type_codes = Vec::with_capacity(entries.len());
    let mut sub_type_code_list_uris = Vec::with_capacity(entries.len());
    let mut total_amounts = Vec::with_capacity(entries.len());
    let mut total_currencies = Vec::with_capacity(entries.len());
    let mut tax_exclusive_amounts = Vec::with_capacity(entries.len());
    let mut tax_exclusive_currencies = Vec::with_capacity(entries.len());
    let mut cpv_codes = Vec::with_capacity(entries.len());
    let mut cpv_code_list_uris = Vec::with_capacity(entries.len());
    let mut country_codes = Vec::with_capacity(entries.len());
    let mut country_code_list_uris = Vec::with_capacity(entries.len());

    for entry in entries {
        names.push(entry.project_name.clone());
        type_codes.push(entry.project_type_code.clone());
        type_code_list_uris.push(entry.project_type_code_list_uri.clone());
        sub_type_codes.push(entry.project_sub_type_code.clone());
        sub_type_code_list_uris.push(entry.project_sub_type_code_list_uri.clone());
        total_amounts.push(entry.project_total_amount.clone());
        total_currencies.push(entry.project_total_currency.clone());
        tax_exclusive_amounts.push(entry.project_tax_exclusive_amount.clone());
        tax_exclusive_currencies.push(entry.project_tax_exclusive_currency.clone());
        cpv_codes.push(entry.project_cpv_code.clone());
        cpv_code_list_uris.push(entry.project_cpv_code_list_uri.clone());
        country_codes.push(entry.project_country_code.clone());
        country_code_list_uris.push(entry.project_country_code_list_uri.clone());
    }

    let df = DataFrame::new(vec![
        Series::new("name", names),
        Series::new("type_code", type_codes),
        Series::new("type_code_list_uri", type_code_list_uris),
        Series::new("sub_type_code", sub_type_codes),
        Series::new("sub_type_code_list_uri", sub_type_code_list_uris),
        Series::new("total_amount", total_amounts),
        Series::new("total_currency", total_currencies),
        Series::new("tax_exclusive_amount", tax_exclusive_amounts),
        Series::new("tax_exclusive_currency", tax_exclusive_currencies),
        Series::new("cpv_code", cpv_codes),
        Series::new("cpv_code_list_uri", cpv_code_list_uris),
        Series::new("country_code", country_codes),
        Series::new("country_code_list_uri", country_code_list_uris),
    ])
    .map_err(|e| AppError::ParseError(format!("Failed to build project struct: {e}")))?;

    Ok(df.into_struct("project").into_series())
}

fn process_to_struct(entries: &[Entry]) -> AppResult<Series> {
    let mut end_dates = Vec::with_capacity(entries.len());
    let mut procedure_codes = Vec::with_capacity(entries.len());
    let mut procedure_code_list_uris = Vec::with_capacity(entries.len());
    let mut urgency_codes = Vec::with_capacity(entries.len());
    let mut urgency_code_list_uris = Vec::with_capacity(entries.len());

    for entry in entries {
        end_dates.push(entry.process_end_date.clone());
        procedure_codes.push(entry.process_procedure_code.clone());
        procedure_code_list_uris.push(entry.process_procedure_code_list_uri.clone());
        urgency_codes.push(entry.process_urgency_code.clone());
        urgency_code_list_uris.push(entry.process_urgency_code_list_uri.clone());
    }

    let df = DataFrame::new(vec![
        Series::new("end_date", end_dates),
        Series::new("procedure_code", procedure_codes),
        Series::new("procedure_code_list_uri", procedure_code_list_uris),
        Series::new("urgency_code", urgency_codes),
        Series::new("urgency_code_list_uri", urgency_code_list_uris),
    ])
    .map_err(|e| AppError::ParseError(format!("Failed to build process struct: {e}")))?;

    Ok(df.into_struct("process").into_series())
}

/// Converts a vector of Entry structs into a Polars DataFrame.
///
/// This function creates a DataFrame from Entry structs, ensuring consistent schema
/// across all DataFrame creations. It uses pre-allocated vectors to minimize allocations.
///
/// # Schema
///
/// Creates 13-14 columns:
/// - `id`, `title`, `link`, `summary`, `updated`, `contract_id`: string columns
/// - `status`: struct(code, list_uri)
/// - `contracting_party`: struct(name, website, type_code, type_code_list_uri, activity_code,
///   activity_code_list_uri, city, zip, country_code, country_code_list_uri)
/// - `project`: struct(name, type_code, type_code_list_uri, sub_type_code, sub_type_code_list_uri,
///   total_amount, total_currency, tax_exclusive_amount, tax_exclusive_currency,
///   cpv_code, cpv_code_list_uri, country_code, country_code_list_uri)
/// - `project_lots`: list(struct(...)) - nested procurement lots with 10 fields each
/// - `tender_results`: list(struct(...)) - nested tender results with 12 fields each
/// - `terms_funding_program`: struct(code, list_uri)
/// - `process`: struct(end_date, procedure_code, procedure_code_list_uri, urgency_code, urgency_code_list_uri)
/// - `cfs_raw_xml` (optional): raw ContractFolderStatus XML when keep_cfs_raw_xml=true
fn entries_to_dataframe(entries: Vec<Entry>, keep_cfs_raw_xml: bool) -> AppResult<DataFrame> {
    let empty: Vec<Option<String>> = Vec::new();
    if entries.is_empty() {
        let empty_list = Series::new("project_lots", Vec::<Series>::new());
        let empty_tender_results = Series::new("tender_results", Vec::<Series>::new());
        let empty_entries: &[Entry] = &[];
        let contracting_party_struct = contracting_party_to_struct(empty_entries)?;
        let project_struct = project_to_struct(empty_entries)?;
        let process_struct = process_to_struct(empty_entries)?;
        let status_struct = status_to_struct(empty_entries)?;
        let terms_struct = terms_funding_program_to_struct(empty_entries)?;

        let mut columns = vec![
            Series::new("id", empty.clone()),
            Series::new("title", empty.clone()),
            Series::new("link", empty.clone()),
            Series::new("summary", empty.clone()),
            Series::new("updated", empty.clone()),
            status_struct,
            Series::new("contract_id", empty.clone()),
            contracting_party_struct,
            project_struct,
            empty_list,
            empty_tender_results,
            terms_struct,
            process_struct,
        ];

        if keep_cfs_raw_xml {
            columns.push(Series::new("cfs_raw_xml", empty));
        }

        return DataFrame::new(columns)
            .map_err(|e| AppError::ParseError(format!("Failed to create DataFrame: {e}")));
    }

    let len = entries.len();
    let mut ids = Vec::with_capacity(len);
    let mut titles = Vec::with_capacity(len);
    let mut links = Vec::with_capacity(len);
    let mut summaries = Vec::with_capacity(len);
    let mut updateds = Vec::with_capacity(len);
    let mut contract_ids = Vec::with_capacity(len);
    let mut project_lots_structs: Vec<Series> = Vec::with_capacity(len);
    let mut cfs_raw_xmls = if keep_cfs_raw_xml {
        Vec::with_capacity(len)
    } else {
        Vec::new()
    };

    for entry in &entries {
        ids.push(entry.id.clone());
        titles.push(entry.title.clone());
        links.push(entry.link.clone());
        summaries.push(entry.summary.clone());
        updateds.push(entry.updated.clone());
        contract_ids.push(entry.contract_id.clone());
        let lot_struct = lots_to_struct_series(&entry.project_lots)?;
        project_lots_structs.push(lot_struct);
        if keep_cfs_raw_xml {
            cfs_raw_xmls.push(entry.cfs_raw_xml.clone());
        }
    }

    let contracting_party_struct = contracting_party_to_struct(&entries)?;
    let project_struct = project_to_struct(&entries)?;
    let process_struct = process_to_struct(&entries)?;
    let status_struct = status_to_struct(&entries)?;
    let terms_struct = terms_funding_program_to_struct(&entries)?;
    let project_lots_series = Series::new("project_lots", project_lots_structs);
    let tender_results_structs = entries
        .iter()
        .map(|entry| tender_results_to_struct_series(&entry.tender_results))
        .collect::<AppResult<Vec<_>>>()?;
    let tender_results_series = Series::new("tender_results", tender_results_structs);

    let mut columns = vec![
        Series::new("id", ids),
        Series::new("title", titles),
        Series::new("link", links),
        Series::new("summary", summaries),
        Series::new("updated", updateds),
        status_struct,
        Series::new("contract_id", contract_ids),
        contracting_party_struct,
        project_struct,
        project_lots_series,
        tender_results_series,
        terms_struct,
        process_struct,
    ];

    if keep_cfs_raw_xml {
        columns.push(Series::new("cfs_raw_xml", cfs_raw_xmls));
    }

    DataFrame::new(columns)
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
/// - Output: `{parquet_dir}/{period}.parquet` (or `{parquet_dir}/{period}/batch_*.parquet` if not concat)
///
/// # Optimizations
///
/// - **Scoped rayon thread pool**: Uses config.parser_threads (or auto-detect) to limit parallelism,
///   reducing context switching and memory overhead in resource-constrained environments.
/// - **Batch processing**: Files are processed in chunks bounded by batch_size, limiting peak DataFrame
///   memory usage.
/// - **Early memory release**: Raw XML bytes are dropped before DataFrame construction to minimize
///   simultaneous memory allocations.
///
/// # Arguments
///
/// * `target_links` - Map of period strings to URLs (used to filter which periods to process)
/// * `procurement_type` - Procurement type determining the extract and parquet directories
/// * `batch_size` - Number of XML files to process per chunk (affects memory usage)
/// * `config` - Resolved configuration containing directory paths and concurrency settings
///
/// # Behavior
///
/// - **Filtering**: Only processes subdirectories whose names match keys in `target_links`
/// - **Skip empty**: Subdirectories with no entries are skipped (logged but not an error)
/// - **Batch output**: Each chunk results in a batch_N.parquet file per period
/// - **Memory controls**: `batch_size` bounds the in-flight DataFrame and `read_concurrency` limits
///   parallel file reads. `parser_threads` limits the rayon thread pool for XML parsing parallelism.
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

    // Configure rayon thread pool for XML parsing.
    // This is critical in Docker environments where available_parallelism() may return the host's CPU count,
    // not the container's limit. By default (parser_threads=0), we auto-detect. Otherwise, use the configured value.
    let num_threads = if config.parser_threads > 0 {
        config.parser_threads
    } else {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    };

    let rayon_pool = ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .map_err(|e| {
            AppError::IoError(format!(
                "Failed to configure rayon thread pool for XML parsing: {e}"
            ))
        })?;

    info!(
        "XML parsing thread pool configured with {} threads",
        num_threads
    );

    // Warn about concat_batches memory usage if enabled.
    if config.concat_batches {
        warn!("concat_batches is enabled: entire periods will be loaded into memory before concatenation. Ensure sufficient RAM is available.");
    }

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

            // Use scoped rayon pool for parallel XML parsing.
            // This respects the configured thread count instead of using the global pool.
            let parsed_entry_batches: Vec<Vec<Entry>> = rayon_pool.install(|| {
                xml_contents
                    .par_iter()
                    .map(|content| parse_xml_bytes(content, config.keep_cfs_raw_xml))
                    .collect::<AppResult<Vec<_>>>()
            })?;

            // Drop raw XML bytes here to free memory before DataFrame construction.
            // This is important for peak memory management: raw XML + parsed entries
            // would otherwise both exist in memory simultaneously.
            drop(xml_contents);

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
            let mut chunk_df = entries_to_dataframe(chunk_entries, config.keep_cfs_raw_xml)?;
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
        parser_threads = num_threads,
        "Parsing completed"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{StatusCode, TermsFundingProgram};

    #[test]
    fn entries_to_dataframe_empty_yields_zero_rows() {
        let df = entries_to_dataframe(vec![], false).unwrap();
        assert_eq!(df.height(), 0);
        assert_eq!(df.width(), 13);
    }

    #[test]
    fn entries_to_dataframe_single_entry() {
        let entry = Entry {
            id: Some("id".to_string()),
            title: Some("title".to_string()),
            link: Some("link".to_string()),
            summary: Some("summary".to_string()),
            updated: Some("2023-01-01".to_string()),
            status: StatusCode::default(),
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
            tender_results: vec![TenderResultRow {
                result_id: Some("1".to_string()),
                result_lot_id: Some("0".to_string()),
                ..Default::default()
            }],
            terms_funding_program: TermsFundingProgram::default(),
            process_end_date: None,
            process_procedure_code: None,
            process_procedure_code_list_uri: None,
            process_urgency_code: None,
            process_urgency_code_list_uri: None,
            cfs_raw_xml: Some("<xml/>".to_string()),
        };

        let df = entries_to_dataframe(vec![entry], true).unwrap();
        assert_eq!(df.height(), 1);
        let tender_results_series = df.column("tender_results").unwrap();
        assert_eq!(tender_results_series.len(), 1);
        assert_eq!(df.width(), 14);
        let lots_col = df.column("project_lots").unwrap();
        assert!(matches!(lots_col.dtype(), DataType::List(_)));
        let contracting_party_col = df.column("contracting_party").unwrap();
        assert!(matches!(contracting_party_col.dtype(), DataType::Struct(_)));
        let project_col = df.column("project").unwrap();
        assert!(matches!(project_col.dtype(), DataType::Struct(_)));
        let process_col = df.column("process").unwrap();
        assert!(matches!(process_col.dtype(), DataType::Struct(_)));
        let value = df.column("id").unwrap().get(0).unwrap();
        assert_eq!(value, AnyValue::String("id"));
    }

    #[test]
    fn entries_to_dataframe_excludes_cfs_raw_xml_when_disabled() {
        let entry = Entry {
            id: Some("id".to_string()),
            title: Some("title".to_string()),
            link: Some("link".to_string()),
            summary: Some("summary".to_string()),
            updated: Some("2023-01-01".to_string()),
            status: StatusCode::default(),
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
            tender_results: Vec::new(),
            terms_funding_program: TermsFundingProgram::default(),
            process_end_date: None,
            process_procedure_code: None,
            process_procedure_code_list_uri: None,
            process_urgency_code: None,
            process_urgency_code_list_uri: None,
            cfs_raw_xml: Some("<xml/>".to_string()),
        };

        let df = entries_to_dataframe(vec![entry], false).unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 13);
        assert!(df.column("cfs_raw_xml").is_err());
    }

    #[test]
    fn entries_to_dataframe_includes_cfs_raw_xml_when_enabled() {
        let entry = Entry {
            id: Some("id".to_string()),
            title: Some("title".to_string()),
            link: Some("link".to_string()),
            summary: Some("summary".to_string()),
            updated: Some("2023-01-01".to_string()),
            status: StatusCode::default(),
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
            tender_results: Vec::new(),
            terms_funding_program: TermsFundingProgram::default(),
            process_end_date: None,
            process_procedure_code: None,
            process_procedure_code_list_uri: None,
            process_urgency_code: None,
            process_urgency_code_list_uri: None,
            cfs_raw_xml: Some("<xml/>".to_string()),
        };

        let df = entries_to_dataframe(vec![entry], true).unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 14);
        let cfs_xml_col = df.column("cfs_raw_xml").unwrap();
        assert_eq!(cfs_xml_col.get(0).unwrap(), AnyValue::String("<xml/>"));
    }
}
