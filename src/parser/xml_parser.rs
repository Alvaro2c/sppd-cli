use super::contract_folder_status::ContractFolderStatusHandler;
use crate::errors::{AppError, AppResult};
use crate::models::{Entry, ProcurementProjectLot};
use quick_xml::events::Event;
use quick_xml::reader::Reader;
#[cfg(test)]
use std::fs;
use std::io::Cursor;
#[cfg(test)]
use std::path::Path;

/// Represents the current field being parsed within an entry
enum EntryField {
    Id,
    Title,
    Summary,
    Updated,
}

/// Builder for constructing Entry structs during XML parsing.
struct EntryBuilder {
    id: Option<String>,
    title: Option<String>,
    link: Option<String>,
    summary: Option<String>,
    updated: Option<String>,
    status_code: Option<String>,
    status_code_list_uri: Option<String>,
    contract_id: Option<String>,
    contracting_party_name: Option<String>,
    contracting_party_website: Option<String>,
    contracting_party_type_code: Option<String>,
    contracting_party_type_code_list_uri: Option<String>,
    contracting_party_activity_code: Option<String>,
    contracting_party_activity_code_list_uri: Option<String>,
    contracting_party_city: Option<String>,
    contracting_party_zip: Option<String>,
    contracting_party_country_code: Option<String>,
    contracting_party_country_code_list_uri: Option<String>,
    project_name: Option<String>,
    project_type_code: Option<String>,
    project_type_code_list_uri: Option<String>,
    project_sub_type_code: Option<String>,
    project_sub_type_code_list_uri: Option<String>,
    project_total_amount: Option<String>,
    project_total_currency: Option<String>,
    project_tax_exclusive_amount: Option<String>,
    project_tax_exclusive_currency: Option<String>,
    project_cpv_code: Option<String>,
    project_cpv_code_list_uri: Option<String>,
    project_country_code: Option<String>,
    project_country_code_list_uri: Option<String>,
    project_lots: Vec<ProcurementProjectLot>,
    result_code: Option<String>,
    result_code_list_uri: Option<String>,
    result_description: Option<String>,
    result_winning_party: Option<String>,
    result_sme_awarded_indicator: Option<String>,
    result_award_date: Option<String>,
    result_tax_exclusive_amount: Option<String>,
    result_tax_exclusive_currency: Option<String>,
    result_payable_amount: Option<String>,
    result_payable_currency: Option<String>,
    terms_funding_program_code: Option<String>,
    terms_funding_program_code_list_uri: Option<String>,
    terms_award_criteria_type_code: Option<String>,
    terms_award_criteria_type_code_list_uri: Option<String>,
    process_end_date: Option<String>,
    process_procedure_code: Option<String>,
    process_procedure_code_list_uri: Option<String>,
    process_urgency_code: Option<String>,
    process_urgency_code_list_uri: Option<String>,
    cfs_raw_xml: Option<String>,
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
            cfs_raw_xml: None,
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
        self.status_code = None;
        self.status_code_list_uri = None;
        self.contract_id = None;
        self.contracting_party_name = None;
        self.contracting_party_website = None;
        self.contracting_party_type_code = None;
        self.contracting_party_type_code_list_uri = None;
        self.contracting_party_activity_code = None;
        self.contracting_party_activity_code_list_uri = None;
        self.contracting_party_city = None;
        self.contracting_party_zip = None;
        self.contracting_party_country_code = None;
        self.contracting_party_country_code_list_uri = None;
        self.project_name = None;
        self.project_type_code = None;
        self.project_type_code_list_uri = None;
        self.project_sub_type_code = None;
        self.project_sub_type_code_list_uri = None;
        self.project_total_amount = None;
        self.project_total_currency = None;
        self.project_tax_exclusive_amount = None;
        self.project_tax_exclusive_currency = None;
        self.project_cpv_code = None;
        self.project_cpv_code_list_uri = None;
        self.project_country_code = None;
        self.project_country_code_list_uri = None;
        self.project_lots.clear();
        self.result_code = None;
        self.result_code_list_uri = None;
        self.result_description = None;
        self.result_winning_party = None;
        self.result_sme_awarded_indicator = None;
        self.result_award_date = None;
        self.result_tax_exclusive_amount = None;
        self.result_tax_exclusive_currency = None;
        self.result_payable_amount = None;
        self.result_payable_currency = None;
        self.terms_funding_program_code = None;
        self.terms_funding_program_code_list_uri = None;
        self.terms_award_criteria_type_code = None;
        self.terms_award_criteria_type_code_list_uri = None;
        self.process_end_date = None;
        self.process_procedure_code = None;
        self.process_procedure_code_list_uri = None;
        self.process_urgency_code = None;
        self.process_urgency_code_list_uri = None;
        self.cfs_raw_xml = None;
        self.current_field = None;
        self.contract_folder_status_handler.reset();
    }

    fn set_field_text(&mut self, text: String) {
        if let Some(ref field) = self.current_field {
            match field {
                EntryField::Id => {
                    let cleaned = text
                        .rsplit('/')
                        .find(|segment| !segment.is_empty())
                        .unwrap_or(&text)
                        .to_string();
                    self.id = Some(cleaned);
                }
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

    fn handle_contract_folder_status_end(&mut self, event: Event) -> AppResult<()> {
        if let Some(p) = self.contract_folder_status_handler.handle_end(event)? {
            self.status_code = p.status_code;
            self.status_code_list_uri = p.status_code_list_uri;
            self.contract_id = p.contract_id;
            self.contracting_party_name = p.contracting_party_name;
            self.contracting_party_website = p.contracting_party_website;
            self.contracting_party_type_code = p.contracting_party_type_code;
            self.contracting_party_type_code_list_uri = p.contracting_party_type_code_list_uri;
            self.contracting_party_activity_code = p.contracting_party_activity_code;
            self.contracting_party_activity_code_list_uri =
                p.contracting_party_activity_code_list_uri;
            self.contracting_party_city = p.contracting_party_city;
            self.contracting_party_zip = p.contracting_party_zip;
            self.contracting_party_country_code = p.contracting_party_country_code;
            self.contracting_party_country_code_list_uri =
                p.contracting_party_country_code_list_uri;
            self.project_name = p.project_name;
            self.project_type_code = p.project_type_code;
            self.project_type_code_list_uri = p.project_type_code_list_uri;
            self.project_sub_type_code = p.project_sub_type_code;
            self.project_sub_type_code_list_uri = p.project_sub_type_code_list_uri;
            self.project_total_amount = p.project_total_amount;
            self.project_total_currency = p.project_total_currency;
            self.project_tax_exclusive_amount = p.project_tax_exclusive_amount;
            self.project_tax_exclusive_currency = p.project_tax_exclusive_currency;
            self.project_cpv_code = p.project_cpv_code;
            self.project_cpv_code_list_uri = p.project_cpv_code_list_uri;
            self.project_country_code = p.project_country_code;
            self.project_country_code_list_uri = p.project_country_code_list_uri;
            self.project_lots = p.project_lots;
            self.result_code = p.result_code;
            self.result_code_list_uri = p.result_code_list_uri;
            self.result_description = p.result_description;
            self.result_winning_party = p.result_winning_party;
            self.result_sme_awarded_indicator = p.result_sme_awarded_indicator;
            self.result_award_date = p.result_award_date;
            self.result_tax_exclusive_amount = p.result_tax_exclusive_amount;
            self.result_tax_exclusive_currency = p.result_tax_exclusive_currency;
            self.result_payable_amount = p.result_payable_amount;
            self.result_payable_currency = p.result_payable_currency;
            self.terms_funding_program_code = p.terms_funding_program_code;
            self.terms_funding_program_code_list_uri = p.terms_funding_program_code_list_uri;
            self.terms_award_criteria_type_code = p.terms_award_criteria_type_code;
            self.terms_award_criteria_type_code_list_uri =
                p.terms_award_criteria_type_code_list_uri;
            self.process_end_date = p.process_end_date;
            self.process_procedure_code = p.process_procedure_code;
            self.process_procedure_code_list_uri = p.process_procedure_code_list_uri;
            self.process_urgency_code = p.process_urgency_code;
            self.process_urgency_code_list_uri = p.process_urgency_code_list_uri;
            self.cfs_raw_xml = Some(p.cfs_raw_xml);
        }
        Ok(())
    }

    fn build(&mut self) -> Option<Entry> {
        if self.id.is_some() || self.title.is_some() {
            Some(Entry {
                id: self.id.take(),
                title: self.title.take(),
                link: self.link.take(),
                summary: self.summary.take(),
                updated: self.updated.take(),
                status_code: self.status_code.take(),
                status_code_list_uri: self.status_code_list_uri.take(),
                contract_id: self.contract_id.take(),
                contracting_party_name: self.contracting_party_name.take(),
                contracting_party_website: self.contracting_party_website.take(),
                contracting_party_type_code: self.contracting_party_type_code.take(),
                contracting_party_type_code_list_uri: self
                    .contracting_party_type_code_list_uri
                    .take(),
                contracting_party_activity_code: self.contracting_party_activity_code.take(),
                contracting_party_activity_code_list_uri: self
                    .contracting_party_activity_code_list_uri
                    .take(),
                contracting_party_city: self.contracting_party_city.take(),
                contracting_party_zip: self.contracting_party_zip.take(),
                contracting_party_country_code: self.contracting_party_country_code.take(),
                contracting_party_country_code_list_uri: self
                    .contracting_party_country_code_list_uri
                    .take(),
                project_name: self.project_name.take(),
                project_type_code: self.project_type_code.take(),
                project_type_code_list_uri: self.project_type_code_list_uri.take(),
                project_sub_type_code: self.project_sub_type_code.take(),
                project_sub_type_code_list_uri: self.project_sub_type_code_list_uri.take(),
                project_total_amount: self.project_total_amount.take(),
                project_total_currency: self.project_total_currency.take(),
                project_tax_exclusive_amount: self.project_tax_exclusive_amount.take(),
                project_tax_exclusive_currency: self.project_tax_exclusive_currency.take(),
                project_cpv_code: self.project_cpv_code.take(),
                project_cpv_code_list_uri: self.project_cpv_code_list_uri.take(),
                project_country_code: self.project_country_code.take(),
                project_country_code_list_uri: self.project_country_code_list_uri.take(),
                project_lots: std::mem::take(&mut self.project_lots),
                result_code: self.result_code.take(),
                result_code_list_uri: self.result_code_list_uri.take(),
                result_description: self.result_description.take(),
                result_winning_party: self.result_winning_party.take(),
                result_sme_awarded_indicator: self.result_sme_awarded_indicator.take(),
                result_award_date: self.result_award_date.take(),
                result_tax_exclusive_amount: self.result_tax_exclusive_amount.take(),
                result_tax_exclusive_currency: self.result_tax_exclusive_currency.take(),
                result_payable_amount: self.result_payable_amount.take(),
                result_payable_currency: self.result_payable_currency.take(),
                terms_funding_program_code: self.terms_funding_program_code.take(),
                terms_funding_program_code_list_uri: self
                    .terms_funding_program_code_list_uri
                    .take(),
                terms_award_criteria_type_code: self.terms_award_criteria_type_code.take(),
                terms_award_criteria_type_code_list_uri: self
                    .terms_award_criteria_type_code_list_uri
                    .take(),
                process_end_date: self.process_end_date.take(),
                process_procedure_code: self.process_procedure_code.take(),
                process_procedure_code_list_uri: self.process_procedure_code_list_uri.take(),
                process_urgency_code: self.process_urgency_code.take(),
                process_urgency_code_list_uri: self.process_urgency_code_list_uri.take(),
                cfs_raw_xml: self.cfs_raw_xml.take(),
            })
        } else {
            None
        }
    }
}

/// Parses XML content provided as bytes.
pub fn parse_xml_bytes(content: &[u8]) -> AppResult<Vec<Entry>> {
    let cursor = Cursor::new(content);
    let mut reader = Reader::from_reader(cursor);
    reader.config_mut().trim_text(true);

    // Estimate capacity from content length (heuristic: ~1 entry per KB)
    let estimated_capacity = (content.len() / 1024).max(100);
    let mut buf = Vec::with_capacity(8192);
    let mut result = Vec::with_capacity(estimated_capacity);

    let mut inside_entry = false;
    let mut builder = EntryBuilder::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => {
                if inside_entry && e.name().as_ref().ends_with(b":ContractFolderStatus") {
                    builder.start_contract_folder_status(Event::Start(e.into_owned()))?;
                    continue;
                }

                if builder.is_inside_contract_folder_status() {
                    builder.handle_contract_folder_status_event(Event::Start(e.into_owned()))?;
                    continue;
                }

                match e.name().as_ref() {
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
                        if let Some(href) = e
                            .attributes()
                            .filter_map(|a| a.ok())
                            .find(|a| a.key.as_ref() == b"href")
                        {
                            let href_str = String::from_utf8_lossy(&href.value);
                            builder.set_link(href_str.into_owned());
                        }
                    }
                    _ => {}
                }
            }
            Event::Empty(e) if inside_entry => {
                if builder.is_inside_contract_folder_status() {
                    builder.handle_contract_folder_status_event(Event::Empty(e.into_owned()))?;
                } else if e.name().as_ref() == b"link" {
                    if let Some(href) = e
                        .attributes()
                        .filter_map(|a| a.ok())
                        .find(|a| a.key.as_ref() == b"href")
                    {
                        let href_str = String::from_utf8_lossy(&href.value);
                        builder.set_link(href_str.into_owned());
                    }
                }
            }
            Event::CData(e) if inside_entry && builder.is_inside_contract_folder_status() => {
                builder.handle_contract_folder_status_event(Event::CData(e.into_owned()))?;
            }
            Event::Comment(e) if inside_entry && builder.is_inside_contract_folder_status() => {
                builder.handle_contract_folder_status_event(Event::Comment(e.into_owned()))?;
            }
            Event::PI(e) if inside_entry && builder.is_inside_contract_folder_status() => {
                builder.handle_contract_folder_status_event(Event::PI(e.into_owned()))?;
            }
            Event::End(e) => {
                if builder.is_inside_contract_folder_status() {
                    if e.name().as_ref().ends_with(b":ContractFolderStatus") {
                        builder.handle_contract_folder_status_end(Event::End(e.into_owned()))?;
                    } else {
                        builder.handle_contract_folder_status_event(Event::End(e.into_owned()))?;
                    }
                    continue;
                }

                match e.name().as_ref() {
                    b"entry" => {
                        inside_entry = false;
                        if let Some(entry) = builder.build() {
                            result.push(entry);
                        }
                        builder.reset();
                    }
                    b"id" | b"title" | b"summary" | b"updated" => {
                        builder.clear_current_field();
                    }
                    _ => {}
                }
            }
            Event::Text(e) if inside_entry => {
                if builder.is_inside_contract_folder_status() {
                    builder.handle_contract_folder_status_event(Event::Text(e.into_owned()))?;
                } else if builder.current_field.is_some() {
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

/// Parses an XML file from disk and delegates to `parse_xml_bytes`.
#[cfg(test)]
pub(crate) fn parse_xml(path: &Path) -> AppResult<Vec<Entry>> {
    let content = fs::read(path)?;
    parse_xml_bytes(&content)
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
