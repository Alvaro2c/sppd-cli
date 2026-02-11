use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Represents a procurement project lot with budget, classification, and location.
///
/// Corresponds to a `<cac:ProcurementProjectLot>` element. All fields are optional to handle
/// variations in the source XML format.
pub struct ProcurementProjectLot {
    /// Lot identifier
    pub id: Option<String>,
    /// Lot name
    pub name: Option<String>,
    /// Total budget amount
    pub total_amount: Option<String>,
    /// Currency of total_amount (currencyID attribute)
    pub total_currency: Option<String>,
    /// Tax-exclusive budget amount
    pub tax_exclusive_amount: Option<String>,
    /// Currency of tax_exclusive_amount (currencyID attribute)
    pub tax_exclusive_currency: Option<String>,
    /// CPV (Common Procurement Vocabulary) code(s), concatenated with `_`
    pub cpv_code: Option<String>,
    /// List URI for CPV code classification
    pub cpv_code_list_uri: Option<String>,
    /// Country code
    pub country_code: Option<String>,
    /// List URI for country code
    pub country_code_list_uri: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Contract folder status code with list URI.
///
/// Represents the `<cbc-place-ext:ContractFolderStatusCode>` element.
pub struct StatusCode {
    /// Status code value
    pub code: Option<String>,
    /// List URI for the status code classification
    pub list_uri: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Funding program code with list URI.
///
/// Represents the `<cac:TenderingTerms>/<cbc:FundingProgramCode>` element.
pub struct TermsFundingProgram {
    /// Funding program code value
    pub code: Option<String>,
    /// List URI for the funding program code classification
    pub list_uri: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Represents one output row derived from a `<cac:TenderResult>` plus an optional lot.
pub struct TenderResultRow {
    /// Artificial ID assigned per TenderResult in document order.
    pub result_id: Option<String>,
    /// Lot identifier from `<cbc:ProcurementProjectLotID>` or `0` when no lot IDs exist.
    pub result_lot_id: Option<String>,
    /// `<cac:TenderResult>/<cbc:ResultCode>`
    pub result_code: Option<String>,
    /// `listURI` attribute for the result code.
    pub result_code_list_uri: Option<String>,
    /// `<cac:TenderResult>/<cbc:Description>`
    pub result_description: Option<String>,
    /// `<cac:TenderResult>/<cac:WinningParty>/<cac:PartyName>/<cbc:Name>`
    pub result_winning_party: Option<String>,
    /// `<cac:TenderResult>/<cbc:SMEAwardedIndicator>`
    pub result_sme_awarded_indicator: Option<String>,
    /// `<cac:TenderResult>/<cbc:AwardDate>`
    pub result_award_date: Option<String>,
    /// `<cac:TenderResult>/<cac:AwardedTenderedProject>/<cac:LegalMonetaryTotal>/<cbc:TaxExclusiveAmount>`
    pub result_tax_exclusive_amount: Option<String>,
    /// `currencyID` attribute from the tax-exclusive amount.
    pub result_tax_exclusive_currency: Option<String>,
    /// `<cac:TenderResult>/<cac:AwardedTenderedProject>/<cac:LegalMonetaryTotal>/<cbc:PayableAmount>`
    pub result_payable_amount: Option<String>,
    /// `currencyID` attribute from the payable amount.
    pub result_payable_currency: Option<String>,
}

/// Represents a single entry element from an XML/Atom feed.
///
/// Corresponds to an `<entry>` element in Atom feeds from Spanish procurement data sources.
/// All fields are optional to handle variations in the source data format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// Atom entry ID
    pub id: Option<String>,
    /// Entry title text
    pub title: Option<String>,
    /// Link href
    pub link: Option<String>,
    /// Entry summary
    pub summary: Option<String>,
    /// Last updated timestamp
    pub updated: Option<String>,
    /// `<cbc-place-ext:ContractFolderStatusCode>`
    pub status: StatusCode,
    /// `<cbc:ContractFolderID>`
    pub contract_id: Option<String>,
    /// `<cac:LocatedContractingParty>/<cac:Party>/<cac:PartyName>/<cbc:Name>`
    pub contracting_party_name: Option<String>,
    /// `<cac:LocatedContractingParty>/<cac:Party>/<cbc:WebsiteURI>`
    pub contracting_party_website: Option<String>,
    /// `<cac:LocatedContractingParty>/<cbc:ContractingPartyTypeCode>`
    pub contracting_party_type_code: Option<String>,
    /// listURI attribute for contracting_party_type_code
    pub contracting_party_type_code_list_uri: Option<String>,
    /// `<cac:LocatedContractingParty>/<cbc:ActivityCode>`
    pub contracting_party_activity_code: Option<String>,
    /// listURI attribute for contracting_party_activity_code
    pub contracting_party_activity_code_list_uri: Option<String>,
    /// `<cac:LocatedContractingParty>/<cac:Party>/<cac:PostalAddress>/<cbc:CityName>`
    pub contracting_party_city: Option<String>,
    /// `<cac:LocatedContractingParty>/<cac:Party>/<cac:PostalAddress>/<cbc:PostalZone>`
    pub contracting_party_zip: Option<String>,
    /// `<cac:LocatedContractingParty>/<cac:Party>/<cac:PostalAddress>/<cac:Country>/<cbc:IdentificationCode>`
    pub contracting_party_country_code: Option<String>,
    /// listURI attribute for contracting_party_country_code
    pub contracting_party_country_code_list_uri: Option<String>,
    /// First `<cbc:Name>` inside `<cac:ProcurementProject>`
    pub project_name: Option<String>,
    /// `<cac:ProcurementProject>/<cbc:TypeCode>`
    pub project_type_code: Option<String>,
    /// listURI attribute for project_type_code
    pub project_type_code_list_uri: Option<String>,
    /// `<cac:ProcurementProject>/<cbc:SubTypeCode>`
    pub project_sub_type_code: Option<String>,
    /// listURI attribute for project_sub_type_code
    pub project_sub_type_code_list_uri: Option<String>,
    /// `<cac:ProcurementProject>/<cac:BudgetAmount>/<cbc:TotalAmount>`
    pub project_total_amount: Option<String>,
    /// Currency of `project_total_amount`
    pub project_total_currency: Option<String>,
    /// `<cac:ProcurementProject>/<cac:BudgetAmount>/<cbc:TaxExclusiveAmount>`
    pub project_tax_exclusive_amount: Option<String>,
    /// Currency of `project_tax_exclusive_amount`
    pub project_tax_exclusive_currency: Option<String>,
    /// Concatenated `<cbc:ItemClassificationCode>` values
    pub project_cpv_code: Option<String>,
    /// listURI attribute for project_cpv_code
    pub project_cpv_code_list_uri: Option<String>,
    /// `<cac:RealizedLocation>/<c:Country>/<cbc:IdentificationCode>`
    pub project_country_code: Option<String>,
    /// listURI attribute for project_country_code
    pub project_country_code_list_uri: Option<String>,
    /// Collection of parsed `<cac:ProcurementProjectLot>` values
    pub project_lots: Vec<ProcurementProjectLot>,
    /// Tender result rows expanded per lot; each row carries the previous `result_*` metadata plus `result_id`/`result_lot_id`.
    pub tender_results: Vec<TenderResultRow>,
    /// `<cac:TenderingTerms>/<cbc:FundingProgramCode>`
    pub terms_funding_program: TermsFundingProgram,
    /// `<cac:TenderingProcess>/<cac:TenderSubmissionDeadlinePeriod>/<cbc:EndDate>`
    pub process_end_date: Option<String>,
    /// `<cac:TenderingProcess>/<cbc:ProcedureCode>`
    pub process_procedure_code: Option<String>,
    /// listURI attribute for process_procedure_code
    pub process_procedure_code_list_uri: Option<String>,
    /// `<cac:TenderingProcess>/<cbc:UrgencyCode>`
    pub process_urgency_code: Option<String>,
    /// listURI attribute for process_urgency_code
    pub process_urgency_code_list_uri: Option<String>,
    /// Entire `<cac-place-ext:ContractFolderStatus>` XML
    pub cfs_raw_xml: Option<String>,
}

// Procurement type aliases
const MINOR_CONTRACTS_ALIASES: &[&str] = &["mc", "minor-contracts", "min"];
const PUBLIC_TENDERS_ALIASES: &[&str] = &["pt", "pub", "public-tenders"];

/// Type of procurement data to download.
///
/// Spanish public procurement data is organized into two main categories:
/// - **Minor Contracts** (`MinorContracts`): Contracts below certain value thresholds
/// - **Public Tenders** (`PublicTenders`): Formal public procurement processes
///
/// This enum is used throughout the CLI to determine which data source to query
/// and where to store downloaded and processed files.
#[derive(Debug, PartialEq, Eq)]
pub enum ProcurementType {
    /// Minor contracts (contratos menores)
    MinorContracts,
    /// Public tenders (licitaciones)
    PublicTenders,
}

impl ProcurementType {
    /// Returns a human-readable name for the procurement type.
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::MinorContracts => "Minor Contracts",
            Self::PublicTenders => "Public Tenders",
        }
    }
    /// Returns the download directory path for the procurement type (for ZIP downloads).
    pub fn download_dir(&self, config: &crate::config::ResolvedConfig) -> PathBuf {
        match self {
            Self::MinorContracts => config.download_dir_mc.clone(),
            Self::PublicTenders => config.download_dir_pt.clone(),
        }
    }

    /// Returns the extraction directory path for the procurement type (for XML extraction).
    pub fn extract_dir(&self, config: &crate::config::ResolvedConfig) -> PathBuf {
        // Extract dir is same as download dir
        self.download_dir(config)
    }

    /// Returns the directory path for the final parquet files.
    pub fn parquet_dir(&self, config: &crate::config::ResolvedConfig) -> PathBuf {
        match self {
            Self::MinorContracts => config.parquet_dir_mc.clone(),
            Self::PublicTenders => config.parquet_dir_pt.clone(),
        }
    }

    /// Checks if a string is a known procurement type alias.
    ///
    /// Returns `true` if the trimmed, lowercased string is in the list of known aliases.
    /// Returns `false` otherwise.
    ///
    /// Known aliases:
    /// - Minor Contracts: `"mc"`, `"minor-contracts"`, `"min"`
    /// - Public Tenders: `"pt"`, `"pub"`, `"public-tenders"`
    pub fn is_known_type(value: &str) -> bool {
        let lower = value.trim().to_lowercase();
        MINOR_CONTRACTS_ALIASES.contains(&lower.as_str())
            || PUBLIC_TENDERS_ALIASES.contains(&lower.as_str())
    }
}

impl From<&str> for ProcurementType {
    /// Converts a string to a `ProcurementType`, accepting various aliases.
    ///
    /// Case-insensitive and trims whitespace. Used by the CLI to parse the `--type` argument.
    ///
    /// **Minor Contracts aliases:** `"mc"`, `"minor-contracts"`, `"min"`
    ///
    /// **Public Tenders aliases:** `"pt"`, `"pub"`, `"public-tenders"`
    ///
    /// Unknown values default to `PublicTenders` (a warning is logged by the CLI).
    /// Use `is_known_type()` to check if a value is recognized before converting.
    fn from(value: &str) -> Self {
        // Trim whitespace and compare case-insensitively
        let lower = value.trim().to_lowercase();

        if MINOR_CONTRACTS_ALIASES.contains(&lower.as_str()) {
            Self::MinorContracts
        } else if PUBLIC_TENDERS_ALIASES.contains(&lower.as_str()) {
            Self::PublicTenders
        } else {
            // Default silently to PublicTenders; callers can decide to log if needed.
            Self::PublicTenders
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ProcurementType;

    #[test]
    fn test_procurement_type_minor_contracts_primary_alias() {
        let proc_type = ProcurementType::from("minor-contracts");
        assert_eq!(proc_type, ProcurementType::MinorContracts);
    }

    #[test]
    fn test_procurement_type_minor_contracts_short_alias() {
        let proc_type = ProcurementType::from("mc");
        assert_eq!(proc_type, ProcurementType::MinorContracts);
    }

    #[test]
    fn test_procurement_type_minor_contracts_min_alias() {
        let proc_type = ProcurementType::from("min");
        assert_eq!(proc_type, ProcurementType::MinorContracts);
    }

    #[test]
    fn test_procurement_type_public_tenders_primary_alias() {
        let proc_type = ProcurementType::from("public-tenders");
        assert_eq!(proc_type, ProcurementType::PublicTenders);
    }

    #[test]
    fn test_procurement_type_public_tenders_short_alias() {
        let proc_type = ProcurementType::from("pt");
        assert_eq!(proc_type, ProcurementType::PublicTenders);
    }

    #[test]
    fn test_procurement_type_public_tenders_pub_alias() {
        let proc_type = ProcurementType::from("pub");
        assert_eq!(proc_type, ProcurementType::PublicTenders);
    }

    #[test]
    fn test_procurement_type_case_insensitive() {
        let proc_type = ProcurementType::from("MINOR-CONTRACTS");
        assert_eq!(proc_type, ProcurementType::MinorContracts);

        let proc_type = ProcurementType::from("Public-Tenders");
        assert_eq!(proc_type, ProcurementType::PublicTenders);
    }

    #[test]
    fn test_procurement_type_unknown_defaults_to_public_tenders() {
        let proc_type = ProcurementType::from("unknown-type");
        assert_eq!(proc_type, ProcurementType::PublicTenders);
    }

    #[test]
    fn test_procurement_type_display_name() {
        let minor = ProcurementType::MinorContracts;
        assert_eq!(minor.display_name(), "Minor Contracts");

        let public = ProcurementType::PublicTenders;
        assert_eq!(public.display_name(), "Public Tenders");
    }

    #[test]
    fn test_procurement_type_empty_string() {
        let proc_type = ProcurementType::from("");
        assert_eq!(proc_type, ProcurementType::PublicTenders);
    }

    #[test]
    fn test_procurement_type_whitespace() {
        let proc_type = ProcurementType::from("   ");
        assert_eq!(proc_type, ProcurementType::PublicTenders);
    }

    #[test]
    fn test_procurement_type_is_known_type_minor_contracts() {
        assert!(ProcurementType::is_known_type("mc"));
        assert!(ProcurementType::is_known_type("minor-contracts"));
        assert!(ProcurementType::is_known_type("min"));
        assert!(ProcurementType::is_known_type("MC"));
        assert!(ProcurementType::is_known_type("MINOR-CONTRACTS"));
        assert!(ProcurementType::is_known_type("  mc  "));
    }

    #[test]
    fn test_procurement_type_is_known_type_public_tenders() {
        assert!(ProcurementType::is_known_type("pt"));
        assert!(ProcurementType::is_known_type("pub"));
        assert!(ProcurementType::is_known_type("public-tenders"));
        assert!(ProcurementType::is_known_type("PT"));
        assert!(ProcurementType::is_known_type("PUBLIC-TENDERS"));
        assert!(ProcurementType::is_known_type("  pub  "));
    }

    #[test]
    fn test_procurement_type_is_known_type_unknown() {
        assert!(!ProcurementType::is_known_type("unknown"));
        assert!(!ProcurementType::is_known_type("invalid-type"));
        assert!(!ProcurementType::is_known_type(""));
        assert!(!ProcurementType::is_known_type("   "));
    }
}
