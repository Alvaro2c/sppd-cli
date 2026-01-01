use crate::errors::{AppError, AppResult};
use crate::models::Entry;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use quick_xml::writer::Writer;
use quick_xml_to_json::xml_to_json;
use std::collections::BTreeMap;
use std::fs::{self, metadata, File};
use std::io::{BufReader, Cursor};
use tracing::{info, warn};

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
///
/// # Example
///
/// ```no_run
/// use sppd_cli::{parser, models::ProcurementType};
/// use std::collections::BTreeMap;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut links = BTreeMap::new();
/// links.insert("202301".to_string(), "https://example.com/202301.zip".to_string());
/// parser::parse_xmls(&links, &ProcurementType::PublicTenders)?;
/// // Processes data/tmp/pt/202301/*.xml -> data/parquet/pt/202301.parquet
/// # Ok(())
/// # }
/// ```
pub fn parse_xmls(
    target_links: &BTreeMap<String, String>,
    procurement_type: &crate::models::ProcurementType,
) -> AppResult<()> {
    let extract_dir = procurement_type.extract_dir();
    let parquet_dir = procurement_type.parquet_dir();

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
        // Pre-allocate with heuristic: assume ~1000 entries per XML file
        let estimated_capacity = xml_files.len().saturating_mul(1000);
        let mut all_entries = Vec::with_capacity(estimated_capacity);
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
        // Note: collect() uses the iterator's size hint (from all_entries.len()) for optimization
        let ids: Vec<Option<String>> = all_entries.iter().map(|e| e.id.clone()).collect();
        let titles: Vec<Option<String>> = all_entries.iter().map(|e| e.title.clone()).collect();
        let links: Vec<Option<String>> = all_entries.iter().map(|e| e.link.clone()).collect();
        let summaries: Vec<Option<String>> =
            all_entries.iter().map(|e| e.summary.clone()).collect();
        let updateds: Vec<Option<String>> = all_entries.iter().map(|e| e.updated.clone()).collect();
        let contract_folder_statuses: Vec<Option<String>> = all_entries
            .iter()
            .map(|e| e.contract_folder_status.clone())
            .collect();

        let mut df = DataFrame::new(vec![
            Series::new("id", ids),
            Series::new("title", titles),
            Series::new("link", links),
            Series::new("summary", summaries),
            Series::new("updated", updateds),
            Series::new("contract_folder_status", contract_folder_statuses),
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
///
/// # Error Handling
///
/// Individual deletion errors are logged as warnings but do not fail the entire operation.
/// The function continues processing remaining files even if some deletions fail.
///
/// # Example
///
/// ```no_run
/// use sppd_cli::{parser, models::ProcurementType};
/// use std::collections::BTreeMap;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut links = BTreeMap::new();
/// links.insert("202301".to_string(), "https://example.com/202301.zip".to_string());
/// // Clean up temporary files, keeping only Parquet files
/// parser::cleanup_files(&links, &ProcurementType::PublicTenders, true).await?;
/// # Ok(())
/// # }
/// ```
pub async fn cleanup_files(
    target_links: &BTreeMap<String, String>,
    procurement_type: &crate::models::ProcurementType,
    should_cleanup: bool,
) -> AppResult<()> {
    if !should_cleanup {
        info!("Cleanup skipped (--cleanup=no)");
        return Ok(());
    }

    let extract_dir = procurement_type.extract_dir();
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

/// Finds all XML/Atom files organized by subdirectory.
///
/// This function scans the immediate subdirectories of the given path and
/// recursively collects all `.xml` and `.atom` files within each subdirectory.
/// Files in the top-level directory are ignored.
///
/// # Returns
///
/// Returns a vector of tuples where:
/// - First element: Subdirectory name (e.g., "202301")
/// - Second element: Vector of paths to XML/Atom files found in that subdirectory
///
/// Only subdirectories containing at least one XML/Atom file are included.
///
/// # Arguments
///
/// * `path` - Base directory to search (typically the extraction directory)
///
/// # Example
///
/// For a directory structure like:
///
/// ```text
/// extract_dir/
///   202301/
///     file1.xml
///     file2.atom
///   202302/
///     nested/
///       file3.xml
/// ```
///
/// The function would return:
/// - `("202301", [file1.xml, file2.atom])`
/// - `("202302", [file3.xml])`
///
/// # Errors
///
/// Returns an error if directory reading fails.
pub fn find_xmls(path: &std::path::Path) -> AppResult<Vec<(String, Vec<std::path::PathBuf>)>> {
    // Pre-allocate with conservative estimate (usually 1-100 subdirectories)
    let mut out = Vec::with_capacity(50);

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
pub(crate) fn collect_xmls(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    // Pre-allocate with conservative estimate (usually 1-20 XML files per directory)
    let mut v = Vec::with_capacity(20);
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

/// Helper function to write an XML event to a buffer
fn write_event_to_buffer(buffer: &mut Vec<u8>, event: Event) -> AppResult<()> {
    let mut writer = Writer::new(buffer);
    writer
        .write_event(event)
        .map_err(|e| AppError::ParseError(format!("Failed to write event to buffer: {e}")))?;
    Ok(())
}

/// Represents the current field being parsed within an entry
enum EntryField {
    Id,
    Title,
    Summary,
    Updated,
}

/// State for capturing ContractFolderStatus XML subtree
struct ContractFolderStatusState {
    depth: u32,
    buffer: Vec<u8>,
    found: bool,
}

/// Builder for constructing Entry structs during XML parsing.
/// Encapsulates the state needed to parse a single entry element.
struct EntryBuilder {
    id: Option<String>,
    title: Option<String>,
    link: Option<String>,
    summary: Option<String>,
    updated: Option<String>,
    contract_folder_status: Option<String>,
    current_field: Option<EntryField>,
    contract_folder_status_state: Option<ContractFolderStatusState>,
}

impl EntryBuilder {
    fn new() -> Self {
        Self {
            id: None,
            title: None,
            link: None,
            summary: None,
            updated: None,
            contract_folder_status: None,
            current_field: None,
            contract_folder_status_state: None,
        }
    }

    fn reset(&mut self) {
        self.id = None;
        self.title = None;
        self.link = None;
        self.summary = None;
        self.updated = None;
        self.contract_folder_status = None;
        self.current_field = None;
        self.contract_folder_status_state = None;
    }

    fn set_field_text(&mut self, text: String) {
        if let Some(ref field) = self.current_field {
            match field {
                EntryField::Id => self.id = Some(text),
                EntryField::Title => self.title = Some(text),
                EntryField::Summary => self.summary = Some(text),
                EntryField::Updated => self.updated = Some(text),
            }
        }
    }

    fn set_link(&mut self, href: String) {
        self.link = Some(href);
    }

    fn set_current_field(&mut self, field: EntryField) {
        self.current_field = Some(field);
    }

    fn clear_current_field(&mut self) {
        self.current_field = None;
    }

    fn is_inside_contract_folder_status(&self) -> bool {
        self.contract_folder_status_state.is_some()
    }

    fn start_contract_folder_status(&mut self, event: Event) -> AppResult<()> {
        if let Some(ref state) = self.contract_folder_status_state {
            if state.found {
                return Err(AppError::ParseError(
                    "Multiple ContractFolderStatus elements found in entry".to_string(),
                ));
            }
        }

        let mut state = ContractFolderStatusState {
            depth: 1,
            buffer: Vec::new(),
            found: true,
        };

        write_event_to_buffer(&mut state.buffer, event)?;
        self.contract_folder_status_state = Some(state);
        Ok(())
    }

    fn handle_contract_folder_status_event(&mut self, event: Event) -> AppResult<()> {
        if let Some(ref mut state) = self.contract_folder_status_state {
            write_event_to_buffer(&mut state.buffer, event)?;
        }
        Ok(())
    }

    fn handle_contract_folder_status_start(&mut self, event: Event) -> AppResult<()> {
        if let Some(ref mut state) = self.contract_folder_status_state {
            state.depth += 1;
            write_event_to_buffer(&mut state.buffer, event)?;
        }
        Ok(())
    }

    fn handle_contract_folder_status_end(&mut self, event: Event) -> AppResult<()> {
        if let Some(ref mut state) = self.contract_folder_status_state {
            write_event_to_buffer(&mut state.buffer, event)?;
            state.depth -= 1;

            if state.depth == 0 {
                // Convert XML buffer to JSON
                let mut json_output = Vec::new();
                let mut cursor = Cursor::new(&state.buffer);
                xml_to_json(&mut cursor, &mut json_output).map_err(|e| {
                    AppError::ParseError(format!(
                        "Failed to convert ContractFolderStatus to JSON: {e}"
                    ))
                })?;

                let json_string = String::from_utf8(json_output).map_err(|e| {
                    AppError::ParseError(format!("Failed to convert JSON to UTF-8: {e}"))
                })?;

                self.contract_folder_status = Some(json_string);
                self.contract_folder_status_state = None;
            }
        }
        Ok(())
    }

    fn build(&mut self) -> Option<Entry> {
        // Only build if at least one key field (id or title) exists
        if self.id.is_some() || self.title.is_some() {
            Some(Entry {
                id: self.id.take(),
                title: self.title.take(),
                link: self.link.take(),
                summary: self.summary.take(),
                updated: self.updated.take(),
                contract_folder_status: self.contract_folder_status.take(),
            })
        } else {
            None
        }
    }
}

/// Parses an XML file and returns a vector of Entry.
pub(crate) fn parse_xml(path: &std::path::Path) -> AppResult<Vec<Entry>> {
    let file = File::open(path)?;
    let mut reader = Reader::from_reader(BufReader::new(file));
    reader.config_mut().trim_text(true);

    // Estimate capacity from file size (heuristic: ~1 entry per KB)
    let estimated_capacity = metadata(path)
        .ok()
        .map(|m| (m.len() as usize / 1024).max(100))
        .unwrap_or(100);

    let mut buf = Vec::new();
    let mut result = Vec::with_capacity(estimated_capacity);

    let mut inside_entry = false;
    let mut builder = EntryBuilder::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => {
                let name = e.name().as_ref().to_vec();

                // Check for ContractFolderStatus element
                if inside_entry && name.ends_with(b":ContractFolderStatus") {
                    builder.start_contract_folder_status(Event::Start(e.clone()))?;
                } else if builder.is_inside_contract_folder_status() {
                    // We're inside ContractFolderStatus, capture this event
                    builder.handle_contract_folder_status_start(Event::Start(e.clone()))?;
                } else {
                    // Handle other elements
                    match name.as_slice() {
                        b"entry" => {
                            inside_entry = true;
                            builder.reset();
                        }
                        b"id" if inside_entry => {
                            builder.set_current_field(EntryField::Id);
                        }
                        b"title" if inside_entry => {
                            builder.set_current_field(EntryField::Title);
                        }
                        b"summary" if inside_entry => {
                            builder.set_current_field(EntryField::Summary);
                        }
                        b"updated" if inside_entry => {
                            builder.set_current_field(EntryField::Updated);
                        }
                        b"link" if inside_entry => {
                            // Get the href attribute
                            if let Some(href) = e
                                .attributes()
                                .filter_map(|a| a.ok())
                                .find(|a| a.key.as_ref() == b"href")
                            {
                                builder.set_link(String::from_utf8_lossy(&href.value).to_string());
                            }
                        }
                        _ => {}
                    }
                }
            }
            Event::Empty(e) if inside_entry => {
                if builder.is_inside_contract_folder_status() {
                    // Capture empty element for ContractFolderStatus
                    builder.handle_contract_folder_status_event(Event::Empty(e.clone()))?;
                } else {
                    // Handle self-closing tags like <link href="..."/>
                    if e.name().as_ref() == b"link" {
                        if let Some(href) = e
                            .attributes()
                            .filter_map(|a| a.ok())
                            .find(|a| a.key.as_ref() == b"href")
                        {
                            builder.set_link(String::from_utf8_lossy(&href.value).to_string());
                        }
                    }
                }
            }
            Event::CData(e) if inside_entry && builder.is_inside_contract_folder_status() => {
                // Capture CDATA for ContractFolderStatus
                builder.handle_contract_folder_status_event(Event::CData(e.clone()))?;
            }
            Event::Comment(e) if inside_entry && builder.is_inside_contract_folder_status() => {
                // Capture comments for ContractFolderStatus
                builder.handle_contract_folder_status_event(Event::Comment(e.clone()))?;
            }
            Event::PI(e) if inside_entry && builder.is_inside_contract_folder_status() => {
                // Capture processing instructions for ContractFolderStatus
                builder.handle_contract_folder_status_event(Event::PI(e.clone()))?;
            }
            Event::End(e) => {
                let name = e.name().as_ref().to_vec();

                // Handle ContractFolderStatus closing
                if builder.is_inside_contract_folder_status() {
                    builder.handle_contract_folder_status_end(Event::End(e.clone()))?;
                }

                // Handle other elements
                match name.as_slice() {
                    b"entry" => {
                        inside_entry = false;
                        // Build and push entry if valid
                        if let Some(entry) = builder.build() {
                            result.push(entry);
                        }
                        builder.reset();
                    }
                    b"id" | b"title" | b"summary" | b"updated"
                        if !builder.is_inside_contract_folder_status() =>
                    {
                        builder.clear_current_field();
                    }
                    _ => {}
                }
            }
            Event::Text(e) if inside_entry => {
                if builder.is_inside_contract_folder_status() {
                    // Capture text for ContractFolderStatus
                    builder.handle_contract_folder_status_event(Event::Text(e.clone()))?;
                } else {
                    // Handle normal text fields
                    let txt = e
                        .decode()
                        .map_err(|e| {
                            AppError::ParseError(format!("Failed to decode XML text: {e}"))
                        })?
                        .into_owned();
                    builder.set_field_text(txt);
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use tempfile::TempDir;

    // Helper function to create a test XML file
    fn create_test_xml_file(path: &std::path::Path, content: &str) {
        let parent = path.parent().unwrap();
        fs::create_dir_all(parent).unwrap();
        fs::File::create(path)
            .unwrap()
            .write_all(content.as_bytes())
            .unwrap();
    }

    #[test]
    fn test_parse_xml_valid_atom_feed() {
        let temp_dir = TempDir::new().unwrap();
        let xml_path = temp_dir.path().join("test.xml");
        // Remove namespace to test basic parsing - namespaces are handled by the parser
        // but may affect attribute matching, so test without namespace first
        let xml_content = r#"<?xml version="1.0"?>
<feed>
  <entry>
    <id>id1</id>
    <title>Title 1</title>
    <link href="http://example.com/1"/>
    <summary>Summary 1</summary>
    <updated>2023-01-01</updated>
  </entry>
  <entry>
    <id>id2</id>
    <title>Title 2</title>
    <link href="http://example.com/2"/>
    <summary>Summary 2</summary>
    <updated>2023-01-02</updated>
  </entry>
</feed>"#;
        create_test_xml_file(&xml_path, xml_content);

        let result = parse_xml(&xml_path).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, Some("id1".to_string()));
        assert_eq!(result[0].title, Some("Title 1".to_string()));
        assert_eq!(result[0].link, Some("http://example.com/1".to_string()));
        assert_eq!(result[0].summary, Some("Summary 1".to_string()));
        assert_eq!(result[0].updated, Some("2023-01-01".to_string()));
        assert_eq!(result[1].id, Some("id2".to_string()));
    }

    #[test]
    fn test_parse_xml_all_fields_populated() {
        let temp_dir = TempDir::new().unwrap();
        let xml_path = temp_dir.path().join("test.xml");
        let xml_content = r#"<?xml version="1.0"?>
<feed>
  <entry>
    <id>full-entry-id</id>
    <title>Full Entry Title</title>
    <link href="https://example.com/full"/>
    <summary>This is a complete summary</summary>
    <updated>2023-06-15T10:30:00Z</updated>
  </entry>
</feed>"#;
        create_test_xml_file(&xml_path, xml_content);

        let result = parse_xml(&xml_path).unwrap();
        assert_eq!(result.len(), 1);
        let entry = &result[0];
        assert_eq!(entry.id, Some("full-entry-id".to_string()));
        assert_eq!(entry.title, Some("Full Entry Title".to_string()));
        assert_eq!(entry.link, Some("https://example.com/full".to_string()));
        assert_eq!(
            entry.summary,
            Some("This is a complete summary".to_string())
        );
        assert_eq!(entry.updated, Some("2023-06-15T10:30:00Z".to_string()));
    }

    #[test]
    fn test_parse_xml_minimal_entry_id_only() {
        let temp_dir = TempDir::new().unwrap();
        let xml_path = temp_dir.path().join("test.xml");
        let xml_content = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>minimal-id</id>
  </entry>
</feed>"#;
        create_test_xml_file(&xml_path, xml_content);

        let result = parse_xml(&xml_path).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, Some("minimal-id".to_string()));
        assert_eq!(result[0].title, None);
        assert_eq!(result[0].link, None);
    }

    #[test]
    fn test_parse_xml_minimal_entry_title_only() {
        let temp_dir = TempDir::new().unwrap();
        let xml_path = temp_dir.path().join("test.xml");
        let xml_content = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>Title Only</title>
  </entry>
</feed>"#;
        create_test_xml_file(&xml_path, xml_content);

        let result = parse_xml(&xml_path).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].title, Some("Title Only".to_string()));
        assert_eq!(result[0].id, None);
    }

    #[test]
    fn test_parse_xml_entry_missing_href() {
        let temp_dir = TempDir::new().unwrap();
        let xml_path = temp_dir.path().join("test.xml");
        let xml_content = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>no-href</id>
    <title>No Link</title>
    <link/>
  </entry>
</feed>"#;
        create_test_xml_file(&xml_path, xml_content);

        let result = parse_xml(&xml_path).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].link, None);
    }

    #[test]
    fn test_parse_xml_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let xml_path = temp_dir.path().join("test.xml");
        let xml_content = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
</feed>"#;
        create_test_xml_file(&xml_path, xml_content);

        let result = parse_xml(&xml_path).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_parse_xml_no_entries() {
        let temp_dir = TempDir::new().unwrap();
        let xml_path = temp_dir.path().join("test.xml");
        let xml_content = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Feed Title</title>
  <updated>2023-01-01</updated>
</feed>"#;
        create_test_xml_file(&xml_path, xml_content);

        let result = parse_xml(&xml_path).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_parse_xml_malformed() {
        let temp_dir = TempDir::new().unwrap();
        let xml_path = temp_dir.path().join("test.xml");
        let xml_content = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>unclosed
  </entry>
</feed>"#;
        create_test_xml_file(&xml_path, xml_content);

        let result = parse_xml(&xml_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_xml_entry_with_nested_text() {
        let temp_dir = TempDir::new().unwrap();
        let xml_path = temp_dir.path().join("test.xml");
        let xml_content = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>nested</id>
    <title>Title with <![CDATA[special characters & <tags>]]></title>
    <summary>Summary with &amp; entities</summary>
  </entry>
</feed>"#;
        create_test_xml_file(&xml_path, xml_content);

        let result = parse_xml(&xml_path).unwrap();
        assert_eq!(result.len(), 1);
        // The parser should handle CDATA and entities
        assert!(result[0].title.is_some());
        assert!(result[0].summary.is_some());
    }

    #[test]
    fn test_collect_xmls_recursive() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().join("base");
        fs::create_dir_all(&base_dir).unwrap();

        // Create nested structure
        let subdir = base_dir.join("subdir");
        fs::create_dir_all(&subdir).unwrap();
        fs::create_dir_all(subdir.join("nested")).unwrap();

        // Create XML and ATOM files at different levels
        create_test_xml_file(&base_dir.join("file1.xml"), "<feed></feed>");
        create_test_xml_file(&subdir.join("file2.xml"), "<feed></feed>");
        create_test_xml_file(&subdir.join("nested/file3.atom"), "<feed></feed>");
        create_test_xml_file(&base_dir.join("file.txt"), "not xml");
        create_test_xml_file(&base_dir.join("file.XML"), "<feed></feed>");
        create_test_xml_file(&base_dir.join("file.ATOM"), "<feed></feed>");

        let files = collect_xmls(&base_dir);
        assert_eq!(files.len(), 5); // file1.xml, file2.xml, file3.atom, file.XML, file.ATOM
        assert!(files.iter().any(|p| p.ends_with("file1.xml")));
        assert!(files.iter().any(|p| p.ends_with("file2.xml")));
        assert!(files.iter().any(|p| p.ends_with("file3.atom")));
        assert!(files.iter().any(|p| p.ends_with("file.XML")));
        assert!(files.iter().any(|p| p.ends_with("file.ATOM")));
        assert!(!files.iter().any(|p| p.ends_with("file.txt")));
    }

    #[test]
    fn test_collect_xmls_case_insensitive() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().join("base");
        fs::create_dir_all(&base_dir).unwrap();

        create_test_xml_file(&base_dir.join("lower.xml"), "<feed></feed>");
        create_test_xml_file(&base_dir.join("UPPER.XML"), "<feed></feed>");
        create_test_xml_file(&base_dir.join("Mixed.Xml"), "<feed></feed>");
        create_test_xml_file(&base_dir.join("lower.atom"), "<feed></feed>");
        create_test_xml_file(&base_dir.join("UPPER.ATOM"), "<feed></feed>");
        create_test_xml_file(&base_dir.join("Mixed.Atom"), "<feed></feed>");

        let files = collect_xmls(&base_dir);
        assert_eq!(files.len(), 6);
    }

    #[test]
    fn test_find_xmls_with_subdirectories() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().join("extract");
        fs::create_dir_all(&base_dir).unwrap();

        // Create subdirectories
        let subdir1 = base_dir.join("202301");
        let subdir2 = base_dir.join("202302");
        fs::create_dir_all(&subdir1).unwrap();
        fs::create_dir_all(&subdir2).unwrap();

        // Add XML files to subdirectories
        create_test_xml_file(&subdir1.join("file1.xml"), "<feed></feed>");
        create_test_xml_file(&subdir1.join("file2.xml"), "<feed></feed>");
        create_test_xml_file(&subdir2.join("file1.atom"), "<feed></feed>");

        // Add non-XML file (should be ignored)
        create_test_xml_file(&subdir2.join("file.txt"), "text");

        // Add file at top level (should be ignored)
        create_test_xml_file(&base_dir.join("top.xml"), "<feed></feed>");

        let result = find_xmls(&base_dir).unwrap();
        assert_eq!(result.len(), 2);

        let (name1, files1) = result.iter().find(|(n, _)| n == "202301").unwrap();
        assert_eq!(name1, "202301");
        assert_eq!(files1.len(), 2);

        let (name2, files2) = result.iter().find(|(n, _)| n == "202302").unwrap();
        assert_eq!(name2, "202302");
        assert_eq!(files2.len(), 1);
    }

    #[test]
    fn test_find_xmls_empty_directories() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().join("extract");
        fs::create_dir_all(&base_dir).unwrap();

        // Create empty subdirectory
        fs::create_dir_all(base_dir.join("empty")).unwrap();

        // Create subdirectory with only non-XML files
        let no_xml_dir = base_dir.join("no_xml");
        fs::create_dir_all(&no_xml_dir).unwrap();
        create_test_xml_file(&no_xml_dir.join("file.txt"), "text");

        let result = find_xmls(&base_dir).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_find_xmls_nested_structure() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().join("extract");
        fs::create_dir_all(&base_dir).unwrap();

        let subdir = base_dir.join("202301");
        fs::create_dir_all(&subdir).unwrap();
        fs::create_dir_all(subdir.join("level1/level2")).unwrap();

        create_test_xml_file(&subdir.join("file1.xml"), "<feed></feed>");
        create_test_xml_file(&subdir.join("level1/file2.xml"), "<feed></feed>");
        create_test_xml_file(&subdir.join("level1/level2/file3.atom"), "<feed></feed>");

        let result = find_xmls(&base_dir).unwrap();
        assert_eq!(result.len(), 1);
        let (_, files) = &result[0];
        assert_eq!(files.len(), 3);
    }
}
