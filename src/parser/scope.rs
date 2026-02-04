use crate::errors::{AppError, AppResult};
use crate::models::{ProcurementProjectLot, TenderResultRow};
use quick_xml::events::{BytesStart, Event};
use quick_xml::writer::Writer;
use std::io::Cursor;

/// Result from finishing a ContractFolderStatus scope.
pub struct ScopeResult {
    pub status_code: Option<String>,
    pub status_code_list_uri: Option<String>,
    pub contract_id: Option<String>,
    pub contracting_party_name: Option<String>,
    pub contracting_party_website: Option<String>,
    pub contracting_party_type_code: Option<String>,
    pub contracting_party_type_code_list_uri: Option<String>,
    pub contracting_party_activity_code: Option<String>,
    pub contracting_party_activity_code_list_uri: Option<String>,
    pub contracting_party_city: Option<String>,
    pub contracting_party_zip: Option<String>,
    pub contracting_party_country_code: Option<String>,
    pub contracting_party_country_code_list_uri: Option<String>,
    pub project_name: Option<String>,
    pub project_type_code: Option<String>,
    pub project_type_code_list_uri: Option<String>,
    pub project_sub_type_code: Option<String>,
    pub project_sub_type_code_list_uri: Option<String>,
    pub project_total_amount: Option<String>,
    pub project_total_currency: Option<String>,
    pub project_tax_exclusive_amount: Option<String>,
    pub project_tax_exclusive_currency: Option<String>,
    pub project_cpv_code: Option<String>,
    pub project_cpv_code_list_uri: Option<String>,
    pub project_country_code: Option<String>,
    pub project_country_code_list_uri: Option<String>,
    pub project_lots: Vec<ProcurementProjectLot>,
    pub tender_results: Vec<TenderResultRow>,
    pub terms_funding_program_code: Option<String>,
    pub terms_funding_program_code_list_uri: Option<String>,
    pub terms_award_criteria_type_code: Option<String>,
    pub terms_award_criteria_type_code_list_uri: Option<String>,
    pub process_end_date: Option<String>,
    pub process_procedure_code: Option<String>,
    pub process_procedure_code_list_uri: Option<String>,
    pub process_urgency_code: Option<String>,
    pub process_urgency_code_list_uri: Option<String>,
    pub cfs_raw_xml: String,
}

/// Which text-capturing element is currently active.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    ProjectLotId,
    ProjectLotName,
    ProjectLotTotalAmount,
    ProjectLotTaxExclusiveAmount,
    ProjectLotCpvCode,
    ProjectLotCountryCode,
    ContractingPartyName,
    ContractingPartyWebsite,
    ContractingPartyTypeCode,
    ContractingPartyActivityCode,
    ContractingPartyCity,
    ContractingPartyZipCode,
    ContractingPartyCountryCode,
    ResultCode,
    ResultDescription,
    ResultWinningParty,
    ResultSmeAwardedIndicator,
    ResultAwardDate,
    ResultTaxExclusiveAmount,
    ResultPayableAmount,
    ResultLotId,
    TermsFundingProgramCode,
    TermsAwardCriteriaTypeCode,
    ProcessEndDate,
    ProcessProcedureCode,
    ProcessUrgencyCode,
}

/// Captures the `<ContractFolderStatus>` subtree and extracts specific fields.
pub struct ContractFolderStatusScope {
    // Output fields
    pub status_code: Option<String>,
    pub status_code_list_uri: Option<String>,
    pub contract_id: Option<String>,
    pub contracting_party_name: Option<String>,
    pub contracting_party_website: Option<String>,
    pub contracting_party_type_code: Option<String>,
    pub contracting_party_type_code_list_uri: Option<String>,
    pub contracting_party_activity_code: Option<String>,
    pub contracting_party_activity_code_list_uri: Option<String>,
    pub contracting_party_city: Option<String>,
    pub contracting_party_zip: Option<String>,
    pub contracting_party_country_code: Option<String>,
    pub contracting_party_country_code_list_uri: Option<String>,
    pub project_name: Option<String>,
    pub project_type_code: Option<String>,
    pub project_type_code_list_uri: Option<String>,
    pub project_sub_type_code: Option<String>,
    pub project_sub_type_code_list_uri: Option<String>,
    pub project_total_amount: Option<String>,
    pub project_total_currency: Option<String>,
    pub project_tax_exclusive_amount: Option<String>,
    pub project_tax_exclusive_currency: Option<String>,
    pub project_cpv_code: Option<String>,
    pub project_cpv_code_list_uri: Option<String>,
    pub project_country_code: Option<String>,
    pub project_country_code_list_uri: Option<String>,
    pub project_lots: Vec<ProcurementProjectLot>,
    pub current_lot: Option<ProcurementProjectLot>,
    pub tender_results: Vec<TenderResultRow>,
    pub current_tender_result: Option<TenderResultRow>,
    pub current_tender_result_lot_ids: Vec<String>,
    pub tender_result_counter: i32,
    tender_result_lot_id_buffer: Option<String>,
    pub terms_funding_program_code: Option<String>,
    pub terms_funding_program_code_list_uri: Option<String>,
    pub terms_award_criteria_type_code: Option<String>,
    pub terms_award_criteria_type_code_list_uri: Option<String>,
    pub process_end_date: Option<String>,
    pub process_procedure_code: Option<String>,
    pub process_procedure_code_list_uri: Option<String>,
    pub process_urgency_code: Option<String>,
    pub process_urgency_code_list_uri: Option<String>,

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
            current_lot: None,
            tender_results: Vec::new(),
            current_tender_result: None,
            current_tender_result_lot_ids: Vec::new(),
            tender_result_counter: 0,
            tender_result_lot_id_buffer: None,
            terms_funding_program_code: None,
            terms_funding_program_code_list_uri: None,
            terms_award_criteria_type_code: None,
            terms_award_criteria_type_code_list_uri: None,
            process_end_date: None,
            process_procedure_code: None,
            process_procedure_code_list_uri: None,
            process_urgency_code: None,
            process_urgency_code_list_uri: None,
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
                let mut field = self.determine_active_field(name);
                if field.is_none()
                    && self.in_project_lot
                    && matches_local_name(name, b"ID")
                    && Self::has_attribute_value(e, b"schemeName", b"ID_LOTE")
                {
                    field = Some(ActiveField::ProjectLotId);
                }
                if let Some(field) = field {
                    if field == ActiveField::ResultLotId {
                        self.tender_result_lot_id_buffer = None;
                    } else {
                        self.prepare_multivalue(field);
                        self.capture_currency(field, e);
                        self.capture_list_uri(field, e);
                    }
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
                    if field == ActiveField::ResultLotId {
                        self.tender_result_lot_id_buffer = Some(String::new());
                        self.push_result_lot_id();
                    } else {
                        self.prepare_multivalue(field);
                        self.ensure_field_exists(field);
                    }
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
                if matches_local_name(name, b"ProcurementProjectLotID") {
                    self.push_result_lot_id();
                }
                if self.in_project_lot
                    && matches_local_name(name, b"Name")
                    && self
                        .current_lot
                        .as_ref()
                        .and_then(|lot| lot.name.as_ref())
                        .is_some()
                {
                    self.project_lot_name_captured = true;
                } else if self.in_project
                    && !self.in_project_lot
                    && matches_local_name(name, b"Name")
                    && self.project_name.is_some()
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
            if self.current_lot.is_some() {
                self.push_current_lot();
            }
            self.current_lot = Some(ProcurementProjectLot::default());
            self.project_lot_name_captured = false;
        } else if matches_local_name(name, b"ProcurementProject") {
            self.in_project = true;
        } else if matches_local_name(name, b"LocatedContractingParty") {
            self.in_contracting_party = true;
        } else if matches_local_name(name, b"TenderResult") {
            self.in_tender_result = true;
            self.start_tender_result();
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
            self.push_current_lot();
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
            self.push_current_tender_result();
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
                ActiveField::ProjectTotalAmount => self.project_total_currency = Some(currency),
                ActiveField::ProjectTaxExclusiveAmount => {
                    self.project_tax_exclusive_currency = Some(currency)
                }
                ActiveField::ProjectLotTotalAmount | ActiveField::ProjectLotTaxExclusiveAmount => {
                    self.set_current_lot_currency(field, currency)
                }
                ActiveField::ResultTaxExclusiveAmount => {
                    self.current_tender_result_mut()
                        .result_tax_exclusive_currency = Some(currency)
                }
                ActiveField::ResultPayableAmount => {
                    self.current_tender_result_mut().result_payable_currency = Some(currency)
                }
                _ => {}
            }
        }
    }

    fn set_current_lot_currency(&mut self, field: ActiveField, currency: String) {
        if let Some(lot) = &mut self.current_lot {
            match field {
                ActiveField::ProjectLotTotalAmount => lot.total_currency = Some(currency),
                ActiveField::ProjectLotTaxExclusiveAmount => {
                    lot.tax_exclusive_currency = Some(currency)
                }
                _ => {}
            }
        }
    }

    fn capture_list_uri(&mut self, field: ActiveField, start: &BytesStart) {
        if let Some(attr) = start
            .attributes()
            .filter_map(|a| a.ok())
            .find(|a| a.key.as_ref() == b"listURI")
        {
            let uri = String::from_utf8_lossy(&attr.value).into_owned();
            match field {
                ActiveField::StatusCode => self.status_code_list_uri = Some(uri),
                ActiveField::ContractingPartyTypeCode => {
                    self.contracting_party_type_code_list_uri = Some(uri)
                }
                ActiveField::ContractingPartyActivityCode => {
                    self.contracting_party_activity_code_list_uri = Some(uri)
                }
                ActiveField::ContractingPartyCountryCode => {
                    self.contracting_party_country_code_list_uri = Some(uri)
                }
                ActiveField::ProjectTypeCode => self.project_type_code_list_uri = Some(uri),
                ActiveField::ProjectSubTypeCode => self.project_sub_type_code_list_uri = Some(uri),
                ActiveField::ProjectCpvCode => self.project_cpv_code_list_uri = Some(uri),
                ActiveField::ProjectCountryCode => self.project_country_code_list_uri = Some(uri),
                ActiveField::ProjectLotCpvCode | ActiveField::ProjectLotCountryCode => {
                    self.set_current_lot_list_uri(field, uri)
                }
                ActiveField::ResultCode => {
                    self.current_tender_result_mut().result_code_list_uri = Some(uri)
                }
                ActiveField::TermsFundingProgramCode => {
                    self.terms_funding_program_code_list_uri = Some(uri)
                }
                ActiveField::TermsAwardCriteriaTypeCode => {
                    self.terms_award_criteria_type_code_list_uri = Some(uri)
                }
                ActiveField::ProcessProcedureCode => {
                    self.process_procedure_code_list_uri = Some(uri)
                }
                ActiveField::ProcessUrgencyCode => self.process_urgency_code_list_uri = Some(uri),
                _ => {} // Non-code fields don't have listURIs
            }
        }
    }

    fn set_current_lot_list_uri(&mut self, field: ActiveField, uri: String) {
        if let Some(lot) = &mut self.current_lot {
            match field {
                ActiveField::ProjectLotCpvCode => lot.cpv_code_list_uri = Some(uri),
                ActiveField::ProjectLotCountryCode => lot.country_code_list_uri = Some(uri),
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
            ActiveField::StatusCode => &mut self.status_code,
            ActiveField::Id => &mut self.contract_id,
            ActiveField::ProjectName => &mut self.project_name,
            ActiveField::ProjectTypeCode => &mut self.project_type_code,
            ActiveField::ProjectSubTypeCode => &mut self.project_sub_type_code,
            ActiveField::ProjectTotalAmount => &mut self.project_total_amount,
            ActiveField::ProjectTaxExclusiveAmount => &mut self.project_tax_exclusive_amount,
            ActiveField::ProjectCpvCode => &mut self.project_cpv_code,
            ActiveField::ProjectCountryCode => &mut self.project_country_code,
            ActiveField::ProjectLotId
            | ActiveField::ProjectLotName
            | ActiveField::ProjectLotTotalAmount
            | ActiveField::ProjectLotTaxExclusiveAmount
            | ActiveField::ProjectLotCpvCode
            | ActiveField::ProjectLotCountryCode => self.project_lot_field_ref(field),
            ActiveField::ContractingPartyName => &mut self.contracting_party_name,
            ActiveField::ContractingPartyWebsite => &mut self.contracting_party_website,
            ActiveField::ContractingPartyTypeCode => &mut self.contracting_party_type_code,
            ActiveField::ContractingPartyActivityCode => &mut self.contracting_party_activity_code,
            ActiveField::ContractingPartyCity => &mut self.contracting_party_city,
            ActiveField::ContractingPartyZipCode => &mut self.contracting_party_zip,
            ActiveField::ContractingPartyCountryCode => &mut self.contracting_party_country_code,
            ActiveField::ResultCode
            | ActiveField::ResultDescription
            | ActiveField::ResultWinningParty
            | ActiveField::ResultSmeAwardedIndicator
            | ActiveField::ResultAwardDate
            | ActiveField::ResultTaxExclusiveAmount
            | ActiveField::ResultPayableAmount => self.tender_result_field_ref(field),
            ActiveField::TermsFundingProgramCode => &mut self.terms_funding_program_code,
            ActiveField::TermsAwardCriteriaTypeCode => &mut self.terms_award_criteria_type_code,
            ActiveField::ProcessEndDate => &mut self.process_end_date,
            ActiveField::ProcessProcedureCode => &mut self.process_procedure_code,
            ActiveField::ProcessUrgencyCode => &mut self.process_urgency_code,
            _ => unreachable!("Invalid active field: {:?}", field),
        }
    }

    fn project_lot_field_ref(&mut self, field: ActiveField) -> &mut Option<String> {
        let lot = self
            .current_lot
            .get_or_insert_with(ProcurementProjectLot::default);
        match field {
            ActiveField::ProjectLotId => &mut lot.id,
            ActiveField::ProjectLotName => &mut lot.name,
            ActiveField::ProjectLotTotalAmount => &mut lot.total_amount,
            ActiveField::ProjectLotTaxExclusiveAmount => &mut lot.tax_exclusive_amount,
            ActiveField::ProjectLotCpvCode => &mut lot.cpv_code,
            ActiveField::ProjectLotCountryCode => &mut lot.country_code,
            _ => unreachable!("Invalid lot field: {:?}", field),
        }
    }

    fn push_current_lot(&mut self) {
        if let Some(lot) = self.current_lot.take() {
            self.project_lots.push(lot);
        }
    }

    fn push_result_lot_id(&mut self) {
        if let Some(buffer) = self.tender_result_lot_id_buffer.take() {
            if !buffer.is_empty() {
                self.current_tender_result_lot_ids.push(buffer);
            }
        }
    }

    fn start_tender_result(&mut self) {
        self.tender_result_counter = self.tender_result_counter.saturating_add(1);
        let row = TenderResultRow {
            result_id: Some(self.tender_result_counter.to_string()),
            ..Default::default()
        };
        self.current_tender_result = Some(row);
        self.current_tender_result_lot_ids.clear();
        self.tender_result_lot_id_buffer = None;
    }

    fn push_current_tender_result(&mut self) {
        if let Some(mut row) = self.current_tender_result.take() {
            self.push_result_lot_id();
            let lot_ids = std::mem::take(&mut self.current_tender_result_lot_ids);
            if lot_ids.is_empty() {
                row.result_lot_id = Some("0".to_string());
                self.tender_results.push(row);
            } else {
                for lot_id in lot_ids {
                    let mut cloned = row.clone();
                    cloned.result_lot_id = Some(lot_id);
                    self.tender_results.push(cloned);
                }
            }
        }
    }

    fn current_tender_result_mut(&mut self) -> &mut TenderResultRow {
        if self.current_tender_result.is_none() {
            let row = TenderResultRow {
                result_id: Some(self.tender_result_counter.to_string()),
                ..Default::default()
            };
            self.current_tender_result = Some(row);
        }
        self.current_tender_result.as_mut().unwrap()
    }

    fn tender_result_field_ref(&mut self, field: ActiveField) -> &mut Option<String> {
        let row = self.current_tender_result_mut();
        match field {
            ActiveField::ResultCode => &mut row.result_code,
            ActiveField::ResultDescription => &mut row.result_description,
            ActiveField::ResultWinningParty => &mut row.result_winning_party,
            ActiveField::ResultSmeAwardedIndicator => &mut row.result_sme_awarded_indicator,
            ActiveField::ResultAwardDate => &mut row.result_award_date,
            ActiveField::ResultTaxExclusiveAmount => &mut row.result_tax_exclusive_amount,
            ActiveField::ResultPayableAmount => &mut row.result_payable_amount,
            _ => unreachable!("Invalid tender result field: {:?}", field),
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

        self.push_current_lot();
        self.push_current_tender_result();

        let cursor = self.writer.into_inner();
        let buffer = cursor.into_inner();
        let raw_xml = String::from_utf8(buffer)
            .map_err(|e| AppError::ParseError(format!("Invalid UTF-8 in XML: {e}")))?;

        Ok(ScopeResult {
            status_code: self.status_code,
            status_code_list_uri: self.status_code_list_uri,
            contract_id: self.contract_id,
            contracting_party_name: self.contracting_party_name,
            contracting_party_website: self.contracting_party_website,
            contracting_party_type_code: self.contracting_party_type_code,
            contracting_party_type_code_list_uri: self.contracting_party_type_code_list_uri,
            contracting_party_activity_code: self.contracting_party_activity_code,
            contracting_party_activity_code_list_uri: self.contracting_party_activity_code_list_uri,
            contracting_party_city: self.contracting_party_city,
            contracting_party_zip: self.contracting_party_zip,
            contracting_party_country_code: self.contracting_party_country_code,
            contracting_party_country_code_list_uri: self.contracting_party_country_code_list_uri,
            project_name: self.project_name,
            project_type_code: self.project_type_code,
            project_type_code_list_uri: self.project_type_code_list_uri,
            project_sub_type_code: self.project_sub_type_code,
            project_sub_type_code_list_uri: self.project_sub_type_code_list_uri,
            project_total_amount: self.project_total_amount,
            project_total_currency: self.project_total_currency,
            project_tax_exclusive_amount: self.project_tax_exclusive_amount,
            project_tax_exclusive_currency: self.project_tax_exclusive_currency,
            project_cpv_code: self.project_cpv_code,
            project_cpv_code_list_uri: self.project_cpv_code_list_uri,
            project_country_code: self.project_country_code,
            project_country_code_list_uri: self.project_country_code_list_uri,
            project_lots: self.project_lots,
            tender_results: self.tender_results,
            terms_funding_program_code: self.terms_funding_program_code,
            terms_funding_program_code_list_uri: self.terms_funding_program_code_list_uri,
            terms_award_criteria_type_code: self.terms_award_criteria_type_code,
            terms_award_criteria_type_code_list_uri: self.terms_award_criteria_type_code_list_uri,
            process_end_date: self.process_end_date,
            process_procedure_code: self.process_procedure_code,
            process_procedure_code_list_uri: self.process_procedure_code_list_uri,
            process_urgency_code: self.process_urgency_code,
            process_urgency_code_list_uri: self.process_urgency_code_list_uri,
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
            if matches_local_name(name, b"ProcurementProjectLotID") {
                return Some(ActiveField::ResultLotId);
            }
            if matches_local_name(name, b"ResultCode") {
                return Some(ActiveField::ResultCode);
            }
            if matches_local_name(name, b"Description") {
                return Some(ActiveField::ResultDescription);
            }
            if self.in_winning_party && self.in_party_name && matches_local_name(name, b"Name") {
                return Some(ActiveField::ResultWinningParty);
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

        if matches!(field, ActiveField::ResultLotId) {
            let buffer = self
                .tender_result_lot_id_buffer
                .get_or_insert_with(String::new);
            buffer.push_str(text);
            return;
        }

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

    fn has_attribute_value(start: &BytesStart, key: &[u8], expected: &[u8]) -> bool {
        start
            .attributes()
            .filter_map(|a| a.ok())
            .any(|attr| attr.key.as_ref() == key && attr.value.as_ref() == expected)
    }
}

/// Checks if a qualified name ends with the given local name.
fn matches_local_name(qname: &[u8], local: &[u8]) -> bool {
    qname.ends_with(local)
        && (qname.len() == local.len()
            || qname.get(qname.len() - local.len() - 1).copied() == Some(b':'))
}
