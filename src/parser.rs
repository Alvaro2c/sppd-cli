use crate::errors::{AppError, AppResult};
use crate::models::Entry;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;
use tracing::info;

pub fn parse_xmls(
    target_links: &BTreeMap<String, String>,
    procurement_type: &crate::models::ProcurementType,
) -> AppResult<()> {
    let extract_dir = Path::new(procurement_type.extract_dir());
    let parquet_dir = Path::new(procurement_type.parquet_dir());

    // Create parquet directory if it doesn't exist
    fs::create_dir_all(parquet_dir)
        .map_err(|e| AppError::IoError(format!("Failed to create parquet directory: {e}")))?;

    // Find all subdirectories with XML/atom files
    let subdirs = find_xmls(extract_dir)?;

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

    // Create progress bar
    let pb = ProgressBar::new(total_subdirs as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
    );

    info!(total = total_subdirs, "Starting XML parsing");

    let mut processed_count = 0;
    let mut skipped_count = 0;

    // Process only subdirectories that match keys in target_links
    for (subdir_name, xml_files) in subdirs_to_process {
        // Update progress bar message
        pb.set_message(format!("Processing {subdir_name}..."));

        // Parse all XML/atom files in this subdirectory
        let mut all_entries = Vec::new();
        for xml_path in xml_files {
            let entries = parse_xml(&xml_path)?;
            all_entries.extend(entries);
        }

        // Skip if no entries found
        if all_entries.is_empty() {
            skipped_count += 1;
            pb.inc(1);
            pb.set_message(format!("Skipped {subdir_name} (no entries)"));
            continue;
        }

        // Convert Entry structs to polars DataFrame
        let ids: Vec<Option<String>> = all_entries.iter().map(|e| e.id.clone()).collect();
        let titles: Vec<Option<String>> = all_entries.iter().map(|e| e.title.clone()).collect();
        let links: Vec<Option<String>> = all_entries.iter().map(|e| e.link.clone()).collect();
        let summaries: Vec<Option<String>> =
            all_entries.iter().map(|e| e.summary.clone()).collect();
        let updateds: Vec<Option<String>> = all_entries.iter().map(|e| e.updated.clone()).collect();

        let mut df = DataFrame::new(vec![
            Series::new("id", ids),
            Series::new("title", titles),
            Series::new("link", links),
            Series::new("summary", summaries),
            Series::new("updated", updateds),
        ])
        .map_err(|e| AppError::ParseError(format!("Failed to create DataFrame: {e}")))?;

        // Create parquet file named after the subdirectory
        let parquet_path = parquet_dir.join(format!("{subdir_name}.parquet"));
        let mut file = File::create(&parquet_path).map_err(|e| {
            AppError::IoError(format!(
                "Failed to create Parquet file {parquet_path:?}: {e}"
            ))
        })?;

        ParquetWriter::new(&mut file)
            .finish(&mut df)
            .map_err(|e| AppError::IoError(format!("Failed to write Parquet file: {e}")))?;

        processed_count += 1;
        pb.inc(1);
        pb.set_message(format!("Completed {subdir_name}"));
    }

    pb.finish_with_message(format!("Processed {processed_count} period(s)"));

    info!(
        processed = processed_count,
        skipped = skipped_count,
        "Parsing completed"
    );

    Ok(())
}

/// For each immediate subdirectory of `path`, returns (subdir_name, Vec<PathBuf>) where the vec contains
/// all `.xml` or `.atom` files under that subdirectory (recursively). Ignores files in the top-level directory.
pub fn find_xmls(path: &std::path::Path) -> AppResult<Vec<(String, Vec<std::path::PathBuf>)>> {
    let mut out = Vec::new();

    for subdir in std::fs::read_dir(path).map_err(AppError::from)? {
        let subdir = subdir.map_err(AppError::from)?;
        let file_type = subdir.file_type().map_err(AppError::from)?;
        if file_type.is_dir() {
            let subdir_path = subdir.path();
            let files = collect_xmls(&subdir_path);
            if !files.is_empty() {
                let name = subdir_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .to_string();
                out.push((name, files));
            }
        }
    }

    Ok(out)
}

/// Recursively collects `.xml` or `.atom` files in a directory (including subdirs).
fn collect_xmls(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut v = Vec::new();
    let walker = walkdir::WalkDir::new(dir).into_iter();
    for entry in walker.flatten() {
        if entry.file_type().is_file() {
            if let Some(ext) = entry.path().extension().and_then(|e| e.to_str()) {
                if ext.eq_ignore_ascii_case("xml") || ext.eq_ignore_ascii_case("atom") {
                    v.push(entry.path().to_path_buf());
                }
            }
        }
    }
    v
}

/// Parses an XML file and returns a vector of Entry.
fn parse_xml(path: &std::path::Path) -> AppResult<Vec<Entry>> {
    let file = File::open(path)?;
    let mut reader = Reader::from_reader(BufReader::new(file));
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut result = Vec::new();

    let mut inside_entry = false;

    // Fields for each entry
    let mut id = None;
    let mut title = None;
    let mut link = None;
    let mut summary = None;
    let mut updated = None;

    // States for nested elements
    let mut inside_id = false;
    let mut inside_title = false;
    let mut inside_summary = false;
    let mut inside_updated = false;

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.name().as_ref() {
                b"entry" => {
                    inside_entry = true;
                    // Reset all fields
                    id = None;
                    title = None;
                    link = None;
                    summary = None;
                    updated = None;
                }
                b"id" if inside_entry => inside_id = true,
                b"title" if inside_entry => inside_title = true,
                b"summary" if inside_entry => inside_summary = true,
                b"updated" if inside_entry => inside_updated = true,
                b"link" if inside_entry => {
                    // Get the href attribute
                    if let Some(href) = e
                        .attributes()
                        .filter_map(|a| a.ok())
                        .find(|a| a.key.as_ref() == b"href")
                    {
                        link = Some(String::from_utf8_lossy(&href.value).to_string());
                    }
                }
                _ => {}
            },
            Event::End(e) => match e.name().as_ref() {
                b"entry" => {
                    inside_entry = false;
                    // Push filled struct if any key field (id or title) exists
                    if id.is_some() || title.is_some() {
                        result.push(Entry {
                            id: id.take(),
                            title: title.take(),
                            link: link.take(),
                            summary: summary.take(),
                            updated: updated.take(),
                        });
                    }
                }
                b"id" => inside_id = false,
                b"title" => inside_title = false,
                b"summary" => inside_summary = false,
                b"updated" => inside_updated = false,
                _ => {}
            },
            Event::Text(e) if inside_entry => {
                let txt = e
                    .decode()
                    .map_err(|e| AppError::ParseError(format!("Failed to decode XML text: {e}")))?
                    .into_owned();
                if inside_id {
                    id = Some(txt);
                } else if inside_title {
                    title = Some(txt);
                } else if inside_summary {
                    summary = Some(txt);
                } else if inside_updated {
                    updated = Some(txt);
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(result)
}
