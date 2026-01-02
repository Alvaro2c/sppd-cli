use super::contract_folder_status::ContractFolderStatusHandler;
use crate::errors::{AppError, AppResult};
use crate::models::Entry;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::fs::{metadata, File};
use std::io::BufReader;

/// Represents the current field being parsed within an entry
enum EntryField {
    Id,
    Title,
    Summary,
    Updated,
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
    contract_folder_status_handler: ContractFolderStatusHandler,
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
            contract_folder_status_handler: ContractFolderStatusHandler::new(),
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
        self.contract_folder_status_handler.reset();
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
        self.contract_folder_status_handler.is_active()
    }

    fn start_contract_folder_status(&mut self, event: Event) -> AppResult<()> {
        self.contract_folder_status_handler.start(event)
    }

    fn handle_contract_folder_status_event(&mut self, event: Event) -> AppResult<()> {
        self.contract_folder_status_handler.handle_event(event)
    }

    fn handle_contract_folder_status_start(&mut self, event: Event) -> AppResult<()> {
        self.contract_folder_status_handler.handle_start(event)
    }

    fn handle_contract_folder_status_end(&mut self, event: Event) -> AppResult<()> {
        if let Some(json_string) = self.contract_folder_status_handler.handle_end(event)? {
            self.contract_folder_status = Some(json_string);
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
}
