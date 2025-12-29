use crate::errors::{AppError, AppResult};
use polars::prelude::*;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;

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

    // Process only subdirectories that match keys in target_links
    for (subdir_name, xml_files) in subdirs {
        if !target_links.contains_key(&subdir_name) {
            continue;
        }

        // Parse all XML/atom files in this subdirectory
        let mut all_titles = Vec::new();
        for xml_path in xml_files {
            let titles = parse_xml(&xml_path)?;
            all_titles.extend(titles);
        }

        // Create parquet file named after the subdirectory
        let parquet_path = parquet_dir.join(format!("{subdir_name}.parquet"));
        let mut df = DataFrame::new(vec![Series::new("title", all_titles)])
            .map_err(|e| AppError::ParseError(format!("Failed to create DataFrame: {e}")))?;

        let mut file = File::create(&parquet_path).map_err(|e| {
            AppError::IoError(format!(
                "Failed to create Parquet file {parquet_path:?}: {e}"
            ))
        })?;

        ParquetWriter::new(&mut file)
            .finish(&mut df)
            .map_err(|e| AppError::IoError(format!("Failed to write Parquet file: {e}")))?;
    }

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

/// Parses an XML file and returns a vector of titles.
fn parse_xml(path: &std::path::Path) -> AppResult<Vec<String>> {
    let file = File::open(path)?;
    let mut reader = Reader::from_reader(BufReader::new(file));
    reader.config_mut().trim_text(true);

    let mut titles = Vec::new();
    let mut buf = Vec::new();
    let mut inside_entry = false;
    let mut inside_title = false;

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.name().as_ref() {
                b"entry" => inside_entry = true,
                b"title" if inside_entry => inside_title = true,
                _ => {}
            },
            Event::End(e) => match e.name().as_ref() {
                b"entry" => inside_entry = false,
                b"title" => inside_title = false,
                _ => {}
            },
            Event::Text(e) if inside_entry && inside_title => {
                titles.push(
                    e.decode()
                        .map_err(|e| {
                            AppError::ParseError(format!("Failed to decode XML text: {e}"))
                        })?
                        .into_owned(),
                );
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(titles)
}
