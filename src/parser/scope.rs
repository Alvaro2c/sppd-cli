use crate::errors::{AppError, AppResult};
use quick_xml::events::Event;
use quick_xml::writer::Writer;
use std::io::Cursor;

/// Result from finishing a ContractFolderStatus scope.
pub struct ScopeResult {
    pub cfs_status_code: Option<String>,
    pub cfs_id: Option<String>,
    pub cfs_project_name: Option<String>,
    pub cfs_project_type_code: Option<String>,
    pub cfs_project_budget_amount: Option<String>,
    pub cfs_project_cpv_codes: Option<String>,
    pub cfs_project_country_code: Option<String>,
    pub cfs_contracting_party_name: Option<String>,
    pub cfs_contracting_party_website: Option<String>,
    pub cfs_contracting_party_type_code: Option<String>,
    pub cfs_tender_result_code: Option<String>,
    pub cfs_tender_result_description: Option<String>,
    pub cfs_tender_result_winning_party: Option<String>,
    pub cfs_tender_result_awarded: Option<String>,
    pub cfs_tendering_process_procedure_code: Option<String>,
    pub cfs_tendering_process_urgency_code: Option<String>,
    pub cfs_raw_xml: String,
}

/// Which text-capturing element is currently active.
#[derive(Clone, Copy)]
enum ActiveField {
    StatusCode,
    Id,
    ProjectName,
    ProjectTypeCode,
    ProjectBudgetAmount,
    ProjectCpvCodes,
    ProjectCountryCode,
    ContractingPartyName,
    ContractingPartyWebsite,
    ContractingPartyTypeCode,
    TenderResultCode,
    TenderResultDescription,
    TenderResultWinningParty,
    TenderResultAwarded,
    TenderingProcedureCode,
    TenderingUrgencyCode,
}

/// Captures the `<ContractFolderStatus>` subtree and extracts specific fields.
pub struct ContractFolderStatusScope {
    // Output fields
    pub cfs_status_code: Option<String>,
    pub cfs_id: Option<String>,
    pub cfs_project_name: Option<String>,
    pub cfs_project_type_code: Option<String>,
    pub cfs_project_budget_amount: Option<String>,
    pub cfs_project_cpv_codes: Option<String>,
    pub cfs_project_country_code: Option<String>,
    pub cfs_contracting_party_name: Option<String>,
    pub cfs_contracting_party_website: Option<String>,
    pub cfs_contracting_party_type_code: Option<String>,
    pub cfs_tender_result_code: Option<String>,
    pub cfs_tender_result_description: Option<String>,
    pub cfs_tender_result_winning_party: Option<String>,
    pub cfs_tender_result_awarded: Option<String>,
    pub cfs_tendering_process_procedure_code: Option<String>,
    pub cfs_tendering_process_urgency_code: Option<String>,

    // Major scope flags
    in_project: bool,
    in_contracting_party: bool,
    in_tender_result: bool,
    in_tendering_process: bool,

    // Sub-scope flags for deeply nested paths
    in_party: bool,
    in_party_name: bool,
    in_winning_party: bool,
    in_country: bool,

    // Currently capturing (for leaf elements with text)
    active_field: Option<ActiveField>,
    project_name_captured: bool,

    // Container capture (for cac: elements with nested children)
    container_capture: Option<ActiveField>,
    container_writer: Option<Writer<Cursor<Vec<u8>>>>,
    container_depth: u32,

    // Raw XML capture
    depth: u32,
    writer: Writer<Cursor<Vec<u8>>>,
}

impl ContractFolderStatusScope {
    /// Creates a new scope initialized with the `<ContractFolderStatus>` start event.
    pub fn start(event: Event) -> AppResult<Self> {
        let cursor = Cursor::new(Vec::with_capacity(16 * 1024));
        let mut writer = Writer::new(cursor);
        writer.write_event(event).map_err(|e| {
            AppError::ParseError(format!("Failed to buffer ContractFolderStatus: {e}"))
        })?;

        Ok(Self {
            cfs_status_code: None,
            cfs_id: None,
            cfs_project_name: None,
            cfs_project_type_code: None,
            cfs_project_budget_amount: None,
            cfs_project_cpv_codes: None,
            cfs_project_country_code: None,
            cfs_contracting_party_name: None,
            cfs_contracting_party_website: None,
            cfs_contracting_party_type_code: None,
            cfs_tender_result_code: None,
            cfs_tender_result_description: None,
            cfs_tender_result_winning_party: None,
            cfs_tender_result_awarded: None,
            cfs_tendering_process_procedure_code: None,
            cfs_tendering_process_urgency_code: None,
            in_project: false,
            in_contracting_party: false,
            in_tender_result: false,
            in_tendering_process: false,
            in_party: false,
            in_party_name: false,
            in_winning_party: false,
            in_country: false,
            active_field: None,
            project_name_captured: false,
            container_capture: None,
            container_writer: None,
            container_depth: 0,
            depth: 1,
            writer,
        })
    }

    /// Handles an event within the `<ContractFolderStatus>` subtree.
    pub fn handle_event(&mut self, event: Event) -> AppResult<()> {
        // Container capture takes precedence - route all events to container writer
        if self.container_capture.is_some() {
            return self.handle_container_event(event);
        }

        match &event {
            Event::Start(e) => {
                self.depth = self.depth.saturating_add(1);
                let qname = e.name();
                let name = qname.as_ref();

                // Check for container element start
                if let Some(field) = self.check_container_start(name) {
                    self.start_container_capture(field, &event)?;
                    return self.write_main_event(event);
                }

                // Major scope entry
                if matches_local_name(name, b"ProcurementProject") {
                    self.in_project = true;
                } else if matches_local_name(name, b"LocatedContractingParty") {
                    self.in_contracting_party = true;
                } else if matches_local_name(name, b"TenderResult") {
                    self.in_tender_result = true;
                } else if matches_local_name(name, b"TenderingProcess") {
                    self.in_tendering_process = true;
                }
                // Sub-scope entry
                else if matches_local_name(name, b"Party") {
                    self.in_party = true;
                } else if matches_local_name(name, b"PartyName") {
                    self.in_party_name = true;
                } else if matches_local_name(name, b"WinningParty") {
                    self.in_winning_party = true;
                } else if matches_local_name(name, b"Country") {
                    self.in_country = true;
                }
                // Leaf elements - set active field
                else {
                    self.active_field = self.determine_active_field(name);
                }
            }
            Event::Empty(e) => {
                let qname = e.name();
                let name = qname.as_ref();
                // Handle self-closing tags as empty captures
                if let Some(field) = self.determine_active_field(name) {
                    self.ensure_field_exists(field);
                }
            }
            Event::Text(text) => {
                if self.active_field.is_some() {
                    let decoded = text
                        .decode()
                        .map_err(|e| AppError::ParseError(format!("Failed to decode text: {e}")))?;
                    self.append_text(&decoded);
                }
            }
            Event::CData(cdata) => {
                if self.active_field.is_some() {
                    let fragment = String::from_utf8_lossy(cdata.as_ref());
                    self.append_text(&fragment);
                }
            }
            Event::End(e) => {
                let qname = e.name();
                let name = qname.as_ref();

                // Clear active field on any end tag
                self.active_field = None;

                // Major scope exit
                if matches_local_name(name, b"ProcurementProject") {
                    self.in_project = false;
                } else if matches_local_name(name, b"LocatedContractingParty") {
                    self.in_contracting_party = false;
                } else if matches_local_name(name, b"TenderResult") {
                    self.in_tender_result = false;
                } else if matches_local_name(name, b"TenderingProcess") {
                    self.in_tendering_process = false;
                }
                // Sub-scope exit
                else if matches_local_name(name, b"Party") {
                    self.in_party = false;
                } else if matches_local_name(name, b"PartyName") {
                    self.in_party_name = false;
                } else if matches_local_name(name, b"WinningParty") {
                    self.in_winning_party = false;
                } else if matches_local_name(name, b"Country") {
                    self.in_country = false;
                }
                // Mark project name as captured when exiting Name in project scope
                else if self.in_project
                    && matches_local_name(name, b"Name")
                    && self.cfs_project_name.is_some()
                {
                    self.project_name_captured = true;
                }

                self.depth = self.depth.checked_sub(1).ok_or_else(|| {
                    AppError::ParseError("ContractFolderStatus depth underflow".to_string())
                })?;
            }
            _ => {}
        }

        self.write_main_event(event)
    }

    /// Checks if an element should trigger container capture.
    fn check_container_start(&self, name: &[u8]) -> Option<ActiveField> {
        if self.in_project {
            if matches_local_name(name, b"BudgetAmount") {
                return Some(ActiveField::ProjectBudgetAmount);
            }
            if matches_local_name(name, b"RequiredCommodityClassification") {
                return Some(ActiveField::ProjectCpvCodes);
            }
        }
        if self.in_tender_result && matches_local_name(name, b"AwardedTenderedProject") {
            return Some(ActiveField::TenderResultAwarded);
        }
        None
    }

    /// Starts capturing a container element's raw XML.
    fn start_container_capture(&mut self, field: ActiveField, event: &Event) -> AppResult<()> {
        let cursor = Cursor::new(Vec::with_capacity(1024));
        let mut writer = Writer::new(cursor);
        writer
            .write_event(event.clone())
            .map_err(|e| AppError::ParseError(format!("Failed to start container capture: {e}")))?;
        self.container_capture = Some(field);
        self.container_writer = Some(writer);
        self.container_depth = 1;
        Ok(())
    }

    /// Handles events while in container capture mode.
    fn handle_container_event(&mut self, event: Event) -> AppResult<()> {
        // Update main depth tracking
        match &event {
            Event::Start(_) => self.depth = self.depth.saturating_add(1),
            Event::End(_) => {
                self.depth = self.depth.checked_sub(1).ok_or_else(|| {
                    AppError::ParseError("ContractFolderStatus depth underflow".to_string())
                })?;
            }
            _ => {}
        }

        // Write to container buffer
        if let Some(ref mut cw) = self.container_writer {
            cw.write_event(event.clone())
                .map_err(|e| AppError::ParseError(format!("Failed to capture container: {e}")))?;
        }

        // Track container depth
        match &event {
            Event::Start(_) => self.container_depth += 1,
            Event::End(_) => {
                self.container_depth -= 1;
                if self.container_depth == 0 {
                    self.finalize_container_capture()?;
                }
            }
            _ => {}
        }

        self.write_main_event(event)
    }

    /// Finalizes container capture and stores the XML string.
    fn finalize_container_capture(&mut self) -> AppResult<()> {
        let field = self.container_capture.take();
        let writer = self.container_writer.take();

        if let (Some(field), Some(w)) = (field, writer) {
            let cursor = w.into_inner();
            let xml = String::from_utf8(cursor.into_inner())
                .map_err(|e| AppError::ParseError(format!("Invalid UTF-8 in container: {e}")))?;

            match field {
                ActiveField::ProjectBudgetAmount => self.cfs_project_budget_amount = Some(xml),
                ActiveField::ProjectCpvCodes => self.cfs_project_cpv_codes = Some(xml),
                ActiveField::TenderResultAwarded => self.cfs_tender_result_awarded = Some(xml),
                _ => {}
            }
        }
        Ok(())
    }

    /// Writes an event to the main XML writer.
    fn write_main_event(&mut self, event: Event) -> AppResult<()> {
        self.writer
            .write_event(event)
            .map_err(|e| AppError::ParseError(format!("Failed to capture XML: {e}")))
    }

    /// Completes the scope and returns all extracted data.
    pub fn finish(mut self, event: Event) -> AppResult<ScopeResult> {
        self.writer
            .write_event(event)
            .map_err(|e| AppError::ParseError(format!("Failed to write closing tag: {e}")))?;

        let cursor = self.writer.into_inner();
        let buffer = cursor.into_inner();
        let raw_xml = String::from_utf8(buffer)
            .map_err(|e| AppError::ParseError(format!("Invalid UTF-8 in XML: {e}")))?;

        Ok(ScopeResult {
            cfs_status_code: self.cfs_status_code,
            cfs_id: self.cfs_id,
            cfs_project_name: self.cfs_project_name,
            cfs_project_type_code: self.cfs_project_type_code,
            cfs_project_budget_amount: self.cfs_project_budget_amount,
            cfs_project_cpv_codes: self.cfs_project_cpv_codes,
            cfs_project_country_code: self.cfs_project_country_code,
            cfs_contracting_party_name: self.cfs_contracting_party_name,
            cfs_contracting_party_website: self.cfs_contracting_party_website,
            cfs_contracting_party_type_code: self.cfs_contracting_party_type_code,
            cfs_tender_result_code: self.cfs_tender_result_code,
            cfs_tender_result_description: self.cfs_tender_result_description,
            cfs_tender_result_winning_party: self.cfs_tender_result_winning_party,
            cfs_tender_result_awarded: self.cfs_tender_result_awarded,
            cfs_tendering_process_procedure_code: self.cfs_tendering_process_procedure_code,
            cfs_tendering_process_urgency_code: self.cfs_tendering_process_urgency_code,
            cfs_raw_xml: raw_xml,
        })
    }

    /// Determines which field to capture based on element name and current scope.
    fn determine_active_field(&self, name: &[u8]) -> Option<ActiveField> {
        // Direct children of ContractFolderStatus
        if matches_local_name(name, b"ContractFolderStatusCode") {
            return Some(ActiveField::StatusCode);
        }
        if matches_local_name(name, b"ContractFolderID") {
            return Some(ActiveField::Id);
        }

        // ProcurementProject children (container elements handled by check_container_start)
        if self.in_project {
            if matches_local_name(name, b"Name") && !self.project_name_captured && !self.in_country
            {
                return Some(ActiveField::ProjectName);
            }
            if matches_local_name(name, b"TypeCode") {
                return Some(ActiveField::ProjectTypeCode);
            }
            if self.in_country && matches_local_name(name, b"IdentificationCode") {
                return Some(ActiveField::ProjectCountryCode);
            }
        }

        // LocatedContractingParty children
        if self.in_contracting_party {
            if matches_local_name(name, b"ContractingPartyTypeCode") {
                return Some(ActiveField::ContractingPartyTypeCode);
            }
            if self.in_party {
                if matches_local_name(name, b"WebsiteURI") {
                    return Some(ActiveField::ContractingPartyWebsite);
                }
                if self.in_party_name && matches_local_name(name, b"Name") {
                    return Some(ActiveField::ContractingPartyName);
                }
            }
        }

        // TenderResult children (AwardedTenderedProject handled by check_container_start)
        if self.in_tender_result {
            if matches_local_name(name, b"ResultCode") {
                return Some(ActiveField::TenderResultCode);
            }
            if matches_local_name(name, b"Description") {
                return Some(ActiveField::TenderResultDescription);
            }
            if self.in_winning_party && self.in_party_name && matches_local_name(name, b"Name") {
                return Some(ActiveField::TenderResultWinningParty);
            }
        }

        // TenderingProcess children
        if self.in_tendering_process {
            if matches_local_name(name, b"ProcedureCode") {
                return Some(ActiveField::TenderingProcedureCode);
            }
            if matches_local_name(name, b"UrgencyCode") {
                return Some(ActiveField::TenderingUrgencyCode);
            }
        }

        None
    }

    /// Appends text to the currently active field.
    fn append_text(&mut self, text: &str) {
        let field = match self.active_field {
            Some(f) => f,
            None => return,
        };

        let target = match field {
            ActiveField::StatusCode => &mut self.cfs_status_code,
            ActiveField::Id => &mut self.cfs_id,
            ActiveField::ProjectName => &mut self.cfs_project_name,
            ActiveField::ProjectTypeCode => &mut self.cfs_project_type_code,
            ActiveField::ProjectBudgetAmount => &mut self.cfs_project_budget_amount,
            ActiveField::ProjectCpvCodes => &mut self.cfs_project_cpv_codes,
            ActiveField::ProjectCountryCode => &mut self.cfs_project_country_code,
            ActiveField::ContractingPartyName => &mut self.cfs_contracting_party_name,
            ActiveField::ContractingPartyWebsite => &mut self.cfs_contracting_party_website,
            ActiveField::ContractingPartyTypeCode => &mut self.cfs_contracting_party_type_code,
            ActiveField::TenderResultCode => &mut self.cfs_tender_result_code,
            ActiveField::TenderResultDescription => &mut self.cfs_tender_result_description,
            ActiveField::TenderResultWinningParty => &mut self.cfs_tender_result_winning_party,
            ActiveField::TenderResultAwarded => &mut self.cfs_tender_result_awarded,
            ActiveField::TenderingProcedureCode => &mut self.cfs_tendering_process_procedure_code,
            ActiveField::TenderingUrgencyCode => &mut self.cfs_tendering_process_urgency_code,
        };

        if let Some(existing) = target {
            existing.push_str(text);
        } else {
            *target = Some(text.to_owned());
        }
    }

    /// Ensures a field exists (for empty elements).
    fn ensure_field_exists(&mut self, field: ActiveField) {
        let target = match field {
            ActiveField::StatusCode => &mut self.cfs_status_code,
            ActiveField::Id => &mut self.cfs_id,
            ActiveField::ProjectName => &mut self.cfs_project_name,
            ActiveField::ProjectTypeCode => &mut self.cfs_project_type_code,
            ActiveField::ProjectBudgetAmount => &mut self.cfs_project_budget_amount,
            ActiveField::ProjectCpvCodes => &mut self.cfs_project_cpv_codes,
            ActiveField::ProjectCountryCode => &mut self.cfs_project_country_code,
            ActiveField::ContractingPartyName => &mut self.cfs_contracting_party_name,
            ActiveField::ContractingPartyWebsite => &mut self.cfs_contracting_party_website,
            ActiveField::ContractingPartyTypeCode => &mut self.cfs_contracting_party_type_code,
            ActiveField::TenderResultCode => &mut self.cfs_tender_result_code,
            ActiveField::TenderResultDescription => &mut self.cfs_tender_result_description,
            ActiveField::TenderResultWinningParty => &mut self.cfs_tender_result_winning_party,
            ActiveField::TenderResultAwarded => &mut self.cfs_tender_result_awarded,
            ActiveField::TenderingProcedureCode => &mut self.cfs_tendering_process_procedure_code,
            ActiveField::TenderingUrgencyCode => &mut self.cfs_tendering_process_urgency_code,
        };
        target.get_or_insert_with(String::new);
    }
}

/// Checks if a qualified name ends with the given local name.
fn matches_local_name(qname: &[u8], local: &[u8]) -> bool {
    qname.ends_with(local)
        && (qname.len() == local.len()
            || qname.get(qname.len() - local.len() - 1).copied() == Some(b':'))
}
