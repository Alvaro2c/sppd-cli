use crate::errors::{AppError, AppResult};
use quick_xml::events::{BytesStart, Event};
use quick_xml::writer::Writer;
use std::io::Cursor;

/// Result from finishing a ContractFolderStatus scope.
pub struct ScopeResult {
    pub cfs_status_code: Option<String>,
    pub cfs_id: Option<String>,
    pub cfs_project_name: Option<String>,
    pub cfs_project_type_code: Option<String>,
    pub cfs_project_sub_type_code: Option<String>,
    pub cfs_project_total_amount: Option<String>,
    pub cfs_project_total_currency: Option<String>,
    pub cfs_project_tax_exclusive_amount: Option<String>,
    pub cfs_project_tax_exclusive_currency: Option<String>,
    pub cfs_project_cpv_codes: Option<String>,
    pub cfs_project_country_code: Option<String>,
    pub cfs_project_lot_name: Option<String>,
    pub cfs_project_lot_total_amount: Option<String>,
    pub cfs_project_lot_total_currency: Option<String>,
    pub cfs_project_lot_tax_exclusive_amount: Option<String>,
    pub cfs_project_lot_tax_exclusive_currency: Option<String>,
    pub cfs_project_lot_cpv_codes: Option<String>,
    pub cfs_project_lot_country_code: Option<String>,
    pub cfs_contracting_party_name: Option<String>,
    pub cfs_contracting_party_website: Option<String>,
    pub cfs_contracting_party_type_code: Option<String>,
    pub cfs_contracting_party_id: Option<String>,
    pub cfs_contracting_party_activity_code: Option<String>,
    pub cfs_contracting_party_city: Option<String>,
    pub cfs_contracting_party_zip_code: Option<String>,
    pub cfs_contracting_party_country_code: Option<String>,
    pub cfs_result_code: Option<String>,
    pub cfs_result_description: Option<String>,
    pub cfs_result_winning_party: Option<String>,
    pub cfs_result_winning_party_id: Option<String>,
    pub cfs_result_sme_awarded_indicator: Option<String>,
    pub cfs_result_award_date: Option<String>,
    pub cfs_result_tax_exclusive_amount: Option<String>,
    pub cfs_result_tax_exclusive_currency: Option<String>,
    pub cfs_result_payable_amount: Option<String>,
    pub cfs_result_payable_currency: Option<String>,
    pub cfs_terms_funding_program_code: Option<String>,
    pub cfs_terms_award_criteria_type_code: Option<String>,
    pub cfs_process_end_date: Option<String>,
    pub cfs_process_procedure_code: Option<String>,
    pub cfs_process_urgency_code: Option<String>,
    pub cfs_raw_xml: String,
}

/// Which text-capturing element is currently active.
#[derive(Clone, Copy)]
enum ActiveField {
    StatusCode,
    Id,
    ProjectName,
    ProjectTypeCode,
    ProjectSubTypeCode,
    ProjectTotalAmount,
    ProjectTaxExclusiveAmount,
    ProjectCpvCode,
    ProjectCountryCode,
    ProjectLotName,
    ProjectLotTotalAmount,
    ProjectLotTaxExclusiveAmount,
    ProjectLotCpvCode,
    ProjectLotCountryCode,
    ContractingPartyName,
    ContractingPartyWebsite,
    ContractingPartyTypeCode,
    ContractingPartyId,
    ContractingPartyActivityCode,
    ContractingPartyCity,
    ContractingPartyZipCode,
    ContractingPartyCountryCode,
    ResultCode,
    ResultDescription,
    ResultWinningParty,
    ResultWinningPartyId,
    ResultSmeAwardedIndicator,
    ResultAwardDate,
    ResultTaxExclusiveAmount,
    ResultPayableAmount,
    TermsFundingProgramCode,
    TermsAwardCriteriaTypeCode,
    ProcessEndDate,
    ProcessProcedureCode,
    ProcessUrgencyCode,
}

/// Captures the `<ContractFolderStatus>` subtree and extracts specific fields.
pub struct ContractFolderStatusScope {
    // Output fields
    pub cfs_status_code: Option<String>,
    pub cfs_id: Option<String>,
    pub cfs_project_name: Option<String>,
    pub cfs_project_type_code: Option<String>,
    pub cfs_project_sub_type_code: Option<String>,
    pub cfs_project_total_amount: Option<String>,
    pub cfs_project_total_currency: Option<String>,
    pub cfs_project_tax_exclusive_amount: Option<String>,
    pub cfs_project_tax_exclusive_currency: Option<String>,
    pub cfs_project_cpv_codes: Option<String>,
    pub cfs_project_country_code: Option<String>,
    pub cfs_project_lot_name: Option<String>,
    pub cfs_project_lot_total_amount: Option<String>,
    pub cfs_project_lot_total_currency: Option<String>,
    pub cfs_project_lot_tax_exclusive_amount: Option<String>,
    pub cfs_project_lot_tax_exclusive_currency: Option<String>,
    pub cfs_project_lot_cpv_codes: Option<String>,
    pub cfs_project_lot_country_code: Option<String>,
    pub cfs_contracting_party_name: Option<String>,
    pub cfs_contracting_party_website: Option<String>,
    pub cfs_contracting_party_type_code: Option<String>,
    pub cfs_contracting_party_id: Option<String>,
    pub cfs_contracting_party_activity_code: Option<String>,
    pub cfs_contracting_party_city: Option<String>,
    pub cfs_contracting_party_zip_code: Option<String>,
    pub cfs_contracting_party_country_code: Option<String>,
    pub cfs_result_code: Option<String>,
    pub cfs_result_description: Option<String>,
    pub cfs_result_winning_party: Option<String>,
    pub cfs_result_winning_party_id: Option<String>,
    pub cfs_result_sme_awarded_indicator: Option<String>,
    pub cfs_result_award_date: Option<String>,
    pub cfs_result_tax_exclusive_amount: Option<String>,
    pub cfs_result_tax_exclusive_currency: Option<String>,
    pub cfs_result_payable_amount: Option<String>,
    pub cfs_result_payable_currency: Option<String>,
    pub cfs_terms_funding_program_code: Option<String>,
    pub cfs_terms_award_criteria_type_code: Option<String>,
    pub cfs_process_end_date: Option<String>,
    pub cfs_process_procedure_code: Option<String>,
    pub cfs_process_urgency_code: Option<String>,

    // Major scope flags
    in_project: bool,
    in_project_lot: bool,
    in_contracting_party: bool,
    in_tender_result: bool,
    in_tendering_process: bool,
    in_tendering_terms: bool,

    // Sub-scope flags for deeply nested paths
    in_party: bool,
    in_party_name: bool,
    in_winning_party: bool,
    in_country: bool,
    in_party_identification: bool,
    in_postal_address: bool,
    in_postal_address_country: bool,
    in_budget_amount: bool,
    in_required_classification: bool,
    in_awarded_tendered_project: bool,
    in_legal_monetary_total: bool,
    in_lot_budget_amount: bool,
    in_lot_required_classification: bool,
    in_lot_country: bool,
    in_awarding_terms: bool,
    in_awarding_criteria: bool,
    in_deadline_period: bool,

    // Currently capturing (for leaf elements with text)
    active_field: Option<ActiveField>,
    project_name_captured: bool,
    project_lot_name_captured: bool,

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
            cfs_project_sub_type_code: None,
            cfs_project_total_amount: None,
            cfs_project_total_currency: None,
            cfs_project_tax_exclusive_amount: None,
            cfs_project_tax_exclusive_currency: None,
            cfs_project_cpv_codes: None,
            cfs_project_country_code: None,
            cfs_project_lot_name: None,
            cfs_project_lot_total_amount: None,
            cfs_project_lot_total_currency: None,
            cfs_project_lot_tax_exclusive_amount: None,
            cfs_project_lot_tax_exclusive_currency: None,
            cfs_project_lot_cpv_codes: None,
            cfs_project_lot_country_code: None,
            cfs_contracting_party_name: None,
            cfs_contracting_party_website: None,
            cfs_contracting_party_type_code: None,
            cfs_contracting_party_id: None,
            cfs_contracting_party_activity_code: None,
            cfs_contracting_party_city: None,
            cfs_contracting_party_zip_code: None,
            cfs_contracting_party_country_code: None,
            cfs_result_code: None,
            cfs_result_description: None,
            cfs_result_winning_party: None,
            cfs_result_winning_party_id: None,
            cfs_result_sme_awarded_indicator: None,
            cfs_result_award_date: None,
            cfs_result_tax_exclusive_amount: None,
            cfs_result_tax_exclusive_currency: None,
            cfs_result_payable_amount: None,
            cfs_result_payable_currency: None,
            cfs_terms_funding_program_code: None,
            cfs_terms_award_criteria_type_code: None,
            cfs_process_end_date: None,
            cfs_process_procedure_code: None,
            cfs_process_urgency_code: None,
            in_project: false,
            in_project_lot: false,
            in_contracting_party: false,
            in_tender_result: false,
            in_tendering_process: false,
            in_tendering_terms: false,
            in_party: false,
            in_party_name: false,
            in_winning_party: false,
            in_country: false,
            in_party_identification: false,
            in_postal_address: false,
            in_postal_address_country: false,
            in_budget_amount: false,
            in_required_classification: false,
            in_awarded_tendered_project: false,
            in_legal_monetary_total: false,
            in_lot_budget_amount: false,
            in_lot_required_classification: false,
            in_lot_country: false,
            in_awarding_terms: false,
            in_awarding_criteria: false,
            in_deadline_period: false,
            active_field: None,
            project_name_captured: false,
            project_lot_name_captured: false,
            depth: 1,
            writer,
        })
    }

    /// Handles an event within the `<ContractFolderStatus>` subtree.
    pub fn handle_event(&mut self, event: Event) -> AppResult<()> {
        match &event {
            Event::Start(e) => {
                self.depth = self.depth.saturating_add(1);
                let qname = e.name();
                let name = qname.as_ref();
                self.update_scope_flags_on_start(name);
                if let Some(field) = self.determine_active_field(name) {
                    self.prepare_multivalue(field);
                    self.capture_currency(field, e);
                    self.active_field = Some(field);
                } else {
                    self.active_field = None;
                }
            }
            Event::Empty(e) => {
                let qname = e.name();
                let name = qname.as_ref();
                self.update_scope_flags_on_start(name);
                if let Some(field) = self.determine_active_field(name) {
                    self.prepare_multivalue(field);
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
                if self.in_project_lot
                    && matches_local_name(name, b"Name")
                    && self.cfs_project_lot_name.is_some()
                {
                    self.project_lot_name_captured = true;
                } else if self.in_project
                    && !self.in_project_lot
                    && matches_local_name(name, b"Name")
                    && self.cfs_project_name.is_some()
                {
                    self.project_name_captured = true;
                }
                self.update_scope_flags_on_end(name);
                self.active_field = None;
                self.depth = self.depth.checked_sub(1).ok_or_else(|| {
                    AppError::ParseError("ContractFolderStatus depth underflow".to_string())
                })?;
            }
            _ => {}
        }

        self.write_main_event(event)
    }

    fn update_scope_flags_on_start(&mut self, name: &[u8]) {
        if matches_local_name(name, b"ProcurementProjectLot") {
            self.in_project_lot = true;
        } else if matches_local_name(name, b"ProcurementProject") {
            self.in_project = true;
        } else if matches_local_name(name, b"LocatedContractingParty") {
            self.in_contracting_party = true;
        } else if matches_local_name(name, b"TenderResult") {
            self.in_tender_result = true;
        } else if matches_local_name(name, b"TenderingProcess") {
            self.in_tendering_process = true;
        } else if matches_local_name(name, b"Party") {
            self.in_party = true;
        } else if matches_local_name(name, b"PartyName") {
            self.in_party_name = true;
        } else if matches_local_name(name, b"WinningParty") {
            self.in_winning_party = true;
        } else if matches_local_name(name, b"PartyIdentification") {
            self.in_party_identification = true;
        } else if matches_local_name(name, b"PostalAddress") {
            self.in_postal_address = true;
        } else if matches_local_name(name, b"Country") {
            if self.in_project_lot {
                self.in_lot_country = true;
            } else if self.in_postal_address {
                self.in_postal_address_country = true;
            } else {
                self.in_country = true;
            }
        } else if matches_local_name(name, b"TenderingTerms") {
            self.in_tendering_terms = true;
        } else if matches_local_name(name, b"AwardingTerms") {
            self.in_awarding_terms = true;
        } else if matches_local_name(name, b"AwardingCriteria") {
            self.in_awarding_criteria = true;
        } else if matches_local_name(name, b"TenderSubmissionDeadlinePeriod") {
            self.in_deadline_period = true;
        }

        if self.in_project && !self.in_project_lot {
            if matches_local_name(name, b"BudgetAmount") {
                self.in_budget_amount = true;
            } else if matches_local_name(name, b"RequiredCommodityClassification") {
                self.in_required_classification = true;
            }
        }

        if self.in_project_lot {
            if matches_local_name(name, b"BudgetAmount") {
                self.in_lot_budget_amount = true;
            } else if matches_local_name(name, b"RequiredCommodityClassification") {
                self.in_lot_required_classification = true;
            }
        }

        if self.in_tender_result {
            if matches_local_name(name, b"AwardedTenderedProject") {
                self.in_awarded_tendered_project = true;
            }
            if self.in_awarded_tendered_project && matches_local_name(name, b"LegalMonetaryTotal") {
                self.in_legal_monetary_total = true;
            }
        }
    }

    fn update_scope_flags_on_end(&mut self, name: &[u8]) {
        if matches_local_name(name, b"ProcurementProjectLot") {
            self.in_project_lot = false;
            self.in_lot_budget_amount = false;
            self.in_lot_required_classification = false;
            self.in_lot_country = false;
        } else if matches_local_name(name, b"ProcurementProject") {
            self.in_project = false;
            self.in_budget_amount = false;
            self.in_required_classification = false;
        } else if matches_local_name(name, b"LocatedContractingParty") {
            self.in_contracting_party = false;
        } else if matches_local_name(name, b"TenderResult") {
            self.in_tender_result = false;
            self.in_awarded_tendered_project = false;
            self.in_legal_monetary_total = false;
        } else if matches_local_name(name, b"TenderingProcess") {
            self.in_tendering_process = false;
        } else if matches_local_name(name, b"Party") {
            self.in_party = false;
        } else if matches_local_name(name, b"PartyName") {
            self.in_party_name = false;
        } else if matches_local_name(name, b"WinningParty") {
            self.in_winning_party = false;
        } else if matches_local_name(name, b"PartyIdentification") {
            self.in_party_identification = false;
        } else if matches_local_name(name, b"PostalAddress") {
            self.in_postal_address = false;
            self.in_postal_address_country = false;
        } else if matches_local_name(name, b"Country") {
            if self.in_project_lot {
                self.in_lot_country = false;
            } else if self.in_postal_address {
                self.in_postal_address_country = false;
            } else {
                self.in_country = false;
            }
        } else if matches_local_name(name, b"TenderingTerms") {
            self.in_tendering_terms = false;
        } else if matches_local_name(name, b"AwardingTerms") {
            self.in_awarding_terms = false;
        } else if matches_local_name(name, b"AwardingCriteria") {
            self.in_awarding_criteria = false;
        } else if matches_local_name(name, b"TenderSubmissionDeadlinePeriod") {
            self.in_deadline_period = false;
        }

        if matches_local_name(name, b"BudgetAmount") {
            self.in_budget_amount = false;
            self.in_lot_budget_amount = false;
        }
        if matches_local_name(name, b"RequiredCommodityClassification") {
            self.in_required_classification = false;
            self.in_lot_required_classification = false;
        }
        if matches_local_name(name, b"AwardedTenderedProject") {
            self.in_awarded_tendered_project = false;
        }
        if matches_local_name(name, b"LegalMonetaryTotal") {
            self.in_legal_monetary_total = false;
        }
    }

    fn capture_currency(&mut self, field: ActiveField, start: &BytesStart) {
        if let Some(attr) = start
            .attributes()
            .filter_map(|a| a.ok())
            .find(|a| a.key.as_ref() == b"currencyID")
        {
            let currency = String::from_utf8_lossy(&attr.value).into_owned();
            match field {
                ActiveField::ProjectTotalAmount => self.cfs_project_total_currency = Some(currency),
                ActiveField::ProjectTaxExclusiveAmount => {
                    self.cfs_project_tax_exclusive_currency = Some(currency)
                }
                ActiveField::ProjectLotTotalAmount => {
                    self.cfs_project_lot_total_currency = Some(currency)
                }
                ActiveField::ProjectLotTaxExclusiveAmount => {
                    self.cfs_project_lot_tax_exclusive_currency = Some(currency)
                }
                ActiveField::ResultTaxExclusiveAmount => {
                    self.cfs_result_tax_exclusive_currency = Some(currency)
                }
                ActiveField::ResultPayableAmount => {
                    self.cfs_result_payable_currency = Some(currency)
                }
                _ => {}
            }
        }
    }

    fn prepare_multivalue(&mut self, field: ActiveField) {
        let target = self.field_ref(field);
        if let Some(existing) = target {
            if !existing.is_empty() {
                existing.push('_');
            }
        }
    }

    fn field_ref(&mut self, field: ActiveField) -> &mut Option<String> {
        match field {
            ActiveField::StatusCode => &mut self.cfs_status_code,
            ActiveField::Id => &mut self.cfs_id,
            ActiveField::ProjectName => &mut self.cfs_project_name,
            ActiveField::ProjectTypeCode => &mut self.cfs_project_type_code,
            ActiveField::ProjectSubTypeCode => &mut self.cfs_project_sub_type_code,
            ActiveField::ProjectTotalAmount => &mut self.cfs_project_total_amount,
            ActiveField::ProjectTaxExclusiveAmount => &mut self.cfs_project_tax_exclusive_amount,
            ActiveField::ProjectCpvCode => &mut self.cfs_project_cpv_codes,
            ActiveField::ProjectCountryCode => &mut self.cfs_project_country_code,
            ActiveField::ProjectLotName => &mut self.cfs_project_lot_name,
            ActiveField::ProjectLotTotalAmount => &mut self.cfs_project_lot_total_amount,
            ActiveField::ProjectLotTaxExclusiveAmount => {
                &mut self.cfs_project_lot_tax_exclusive_amount
            }
            ActiveField::ProjectLotCpvCode => &mut self.cfs_project_lot_cpv_codes,
            ActiveField::ProjectLotCountryCode => &mut self.cfs_project_lot_country_code,
            ActiveField::ContractingPartyName => &mut self.cfs_contracting_party_name,
            ActiveField::ContractingPartyWebsite => &mut self.cfs_contracting_party_website,
            ActiveField::ContractingPartyTypeCode => &mut self.cfs_contracting_party_type_code,
            ActiveField::ContractingPartyId => &mut self.cfs_contracting_party_id,
            ActiveField::ContractingPartyActivityCode => {
                &mut self.cfs_contracting_party_activity_code
            }
            ActiveField::ContractingPartyCity => &mut self.cfs_contracting_party_city,
            ActiveField::ContractingPartyZipCode => &mut self.cfs_contracting_party_zip_code,
            ActiveField::ContractingPartyCountryCode => {
                &mut self.cfs_contracting_party_country_code
            }
            ActiveField::ResultCode => &mut self.cfs_result_code,
            ActiveField::ResultDescription => &mut self.cfs_result_description,
            ActiveField::ResultWinningParty => &mut self.cfs_result_winning_party,
            ActiveField::ResultWinningPartyId => &mut self.cfs_result_winning_party_id,
            ActiveField::ResultSmeAwardedIndicator => &mut self.cfs_result_sme_awarded_indicator,
            ActiveField::ResultAwardDate => &mut self.cfs_result_award_date,
            ActiveField::ResultTaxExclusiveAmount => &mut self.cfs_result_tax_exclusive_amount,
            ActiveField::ResultPayableAmount => &mut self.cfs_result_payable_amount,
            ActiveField::TermsFundingProgramCode => &mut self.cfs_terms_funding_program_code,
            ActiveField::TermsAwardCriteriaTypeCode => &mut self.cfs_terms_award_criteria_type_code,
            ActiveField::ProcessEndDate => &mut self.cfs_process_end_date,
            ActiveField::ProcessProcedureCode => &mut self.cfs_process_procedure_code,
            ActiveField::ProcessUrgencyCode => &mut self.cfs_process_urgency_code,
        }
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
            cfs_project_sub_type_code: self.cfs_project_sub_type_code,
            cfs_project_total_amount: self.cfs_project_total_amount,
            cfs_project_total_currency: self.cfs_project_total_currency,
            cfs_project_tax_exclusive_amount: self.cfs_project_tax_exclusive_amount,
            cfs_project_tax_exclusive_currency: self.cfs_project_tax_exclusive_currency,
            cfs_project_cpv_codes: self.cfs_project_cpv_codes,
            cfs_project_country_code: self.cfs_project_country_code,
            cfs_project_lot_name: self.cfs_project_lot_name,
            cfs_project_lot_total_amount: self.cfs_project_lot_total_amount,
            cfs_project_lot_total_currency: self.cfs_project_lot_total_currency,
            cfs_project_lot_tax_exclusive_amount: self.cfs_project_lot_tax_exclusive_amount,
            cfs_project_lot_tax_exclusive_currency: self.cfs_project_lot_tax_exclusive_currency,
            cfs_project_lot_cpv_codes: self.cfs_project_lot_cpv_codes,
            cfs_project_lot_country_code: self.cfs_project_lot_country_code,
            cfs_contracting_party_name: self.cfs_contracting_party_name,
            cfs_contracting_party_website: self.cfs_contracting_party_website,
            cfs_contracting_party_type_code: self.cfs_contracting_party_type_code,
            cfs_contracting_party_id: self.cfs_contracting_party_id,
            cfs_contracting_party_activity_code: self.cfs_contracting_party_activity_code,
            cfs_contracting_party_city: self.cfs_contracting_party_city,
            cfs_contracting_party_zip_code: self.cfs_contracting_party_zip_code,
            cfs_contracting_party_country_code: self.cfs_contracting_party_country_code,
            cfs_result_code: self.cfs_result_code,
            cfs_result_description: self.cfs_result_description,
            cfs_result_winning_party: self.cfs_result_winning_party,
            cfs_result_winning_party_id: self.cfs_result_winning_party_id,
            cfs_result_sme_awarded_indicator: self.cfs_result_sme_awarded_indicator,
            cfs_result_award_date: self.cfs_result_award_date,
            cfs_result_tax_exclusive_amount: self.cfs_result_tax_exclusive_amount,
            cfs_result_tax_exclusive_currency: self.cfs_result_tax_exclusive_currency,
            cfs_result_payable_amount: self.cfs_result_payable_amount,
            cfs_result_payable_currency: self.cfs_result_payable_currency,
            cfs_terms_funding_program_code: self.cfs_terms_funding_program_code,
            cfs_terms_award_criteria_type_code: self.cfs_terms_award_criteria_type_code,
            cfs_process_end_date: self.cfs_process_end_date,
            cfs_process_procedure_code: self.cfs_process_procedure_code,
            cfs_process_urgency_code: self.cfs_process_urgency_code,
            cfs_raw_xml: raw_xml,
        })
    }

    /// Determines which field to capture based on element name and current scope.
    fn determine_active_field(&self, name: &[u8]) -> Option<ActiveField> {
        if matches_local_name(name, b"ContractFolderStatusCode") {
            return Some(ActiveField::StatusCode);
        }
        if matches_local_name(name, b"ContractFolderID") {
            return Some(ActiveField::Id);
        }

        // ProcurementProjectLot takes precedence when we're inside it
        if self.in_project_lot {
            if matches_local_name(name, b"Name")
                && !self.project_lot_name_captured
                && !self.in_lot_country
            {
                return Some(ActiveField::ProjectLotName);
            }
            if self.in_lot_budget_amount && matches_local_name(name, b"TotalAmount") {
                return Some(ActiveField::ProjectLotTotalAmount);
            }
            if self.in_lot_budget_amount && matches_local_name(name, b"TaxExclusiveAmount") {
                return Some(ActiveField::ProjectLotTaxExclusiveAmount);
            }
            if self.in_lot_required_classification
                && matches_local_name(name, b"ItemClassificationCode")
            {
                return Some(ActiveField::ProjectLotCpvCode);
            }
            if self.in_lot_country && matches_local_name(name, b"IdentificationCode") {
                return Some(ActiveField::ProjectLotCountryCode);
            }
        }

        if self.in_project && !self.in_project_lot {
            if matches_local_name(name, b"Name") && !self.project_name_captured && !self.in_country
            {
                return Some(ActiveField::ProjectName);
            }
            if matches_local_name(name, b"TypeCode") {
                return Some(ActiveField::ProjectTypeCode);
            }
            if matches_local_name(name, b"SubTypeCode") {
                return Some(ActiveField::ProjectSubTypeCode);
            }
            if self.in_budget_amount && matches_local_name(name, b"TotalAmount") {
                return Some(ActiveField::ProjectTotalAmount);
            }
            if self.in_budget_amount && matches_local_name(name, b"TaxExclusiveAmount") {
                return Some(ActiveField::ProjectTaxExclusiveAmount);
            }
            if self.in_required_classification
                && matches_local_name(name, b"ItemClassificationCode")
            {
                return Some(ActiveField::ProjectCpvCode);
            }
            if self.in_country && matches_local_name(name, b"IdentificationCode") {
                return Some(ActiveField::ProjectCountryCode);
            }
        }

        if self.in_contracting_party {
            if matches_local_name(name, b"ContractingPartyTypeCode") {
                return Some(ActiveField::ContractingPartyTypeCode);
            }
            if matches_local_name(name, b"ActivityCode") {
                return Some(ActiveField::ContractingPartyActivityCode);
            }
            if self.in_party {
                if matches_local_name(name, b"WebsiteURI") {
                    return Some(ActiveField::ContractingPartyWebsite);
                }
                if self.in_party_name && matches_local_name(name, b"Name") {
                    return Some(ActiveField::ContractingPartyName);
                }
                if self.in_party_identification && matches_local_name(name, b"ID") {
                    return Some(ActiveField::ContractingPartyId);
                }
                if self.in_postal_address {
                    if matches_local_name(name, b"CityName") {
                        return Some(ActiveField::ContractingPartyCity);
                    }
                    if matches_local_name(name, b"PostalZone") {
                        return Some(ActiveField::ContractingPartyZipCode);
                    }
                    if self.in_postal_address_country
                        && matches_local_name(name, b"IdentificationCode")
                    {
                        return Some(ActiveField::ContractingPartyCountryCode);
                    }
                }
            }
        }

        if self.in_tender_result {
            if matches_local_name(name, b"ResultCode") {
                return Some(ActiveField::ResultCode);
            }
            if matches_local_name(name, b"Description") {
                return Some(ActiveField::ResultDescription);
            }
            if self.in_winning_party && self.in_party_name && matches_local_name(name, b"Name") {
                return Some(ActiveField::ResultWinningParty);
            }
            if self.in_winning_party
                && self.in_party_identification
                && matches_local_name(name, b"ID")
            {
                return Some(ActiveField::ResultWinningPartyId);
            }
            if matches_local_name(name, b"SMEAwardedIndicator") {
                return Some(ActiveField::ResultSmeAwardedIndicator);
            }
            if matches_local_name(name, b"AwardDate") {
                return Some(ActiveField::ResultAwardDate);
            }
        }

        if self.in_legal_monetary_total && matches_local_name(name, b"TaxExclusiveAmount") {
            return Some(ActiveField::ResultTaxExclusiveAmount);
        }
        if self.in_legal_monetary_total && matches_local_name(name, b"PayableAmount") {
            return Some(ActiveField::ResultPayableAmount);
        }

        if self.in_tendering_process {
            if self.in_deadline_period && matches_local_name(name, b"EndDate") {
                return Some(ActiveField::ProcessEndDate);
            }
            if matches_local_name(name, b"ProcedureCode") {
                return Some(ActiveField::ProcessProcedureCode);
            }
            if matches_local_name(name, b"UrgencyCode") {
                return Some(ActiveField::ProcessUrgencyCode);
            }
        }

        if self.in_tendering_terms {
            if matches_local_name(name, b"FundingProgramCode") {
                return Some(ActiveField::TermsFundingProgramCode);
            }
            if self.in_awarding_terms
                && self.in_awarding_criteria
                && matches_local_name(name, b"AwardingCriteriaTypeCode")
            {
                return Some(ActiveField::TermsAwardCriteriaTypeCode);
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

        let target = self.field_ref(field);
        if let Some(existing) = target {
            existing.push_str(text);
        } else {
            *target = Some(text.to_owned());
        }
    }

    /// Ensures a field exists (for empty elements).
    fn ensure_field_exists(&mut self, field: ActiveField) {
        self.field_ref(field).get_or_insert_with(String::new);
    }
}

/// Checks if a qualified name ends with the given local name.
fn matches_local_name(qname: &[u8], local: &[u8]) -> bool {
    qname.ends_with(local)
        && (qname.len() == local.len()
            || qname.get(qname.len() - local.len() - 1).copied() == Some(b':'))
}
