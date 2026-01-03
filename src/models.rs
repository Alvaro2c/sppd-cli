use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Represents a single entry element from an XML/Atom feed.
///
/// Corresponds to an `<entry>` element in Atom feeds from Spanish procurement data sources.
/// All fields are optional to handle variations in the source data format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// The entry ID
    pub id: Option<String>,
    /// The entry title
    pub title: Option<String>,
    /// The link href attribute
    pub link: Option<String>,
    /// The entry summary
    pub summary: Option<String>,
    /// The last updated timestamp
    pub updated: Option<String>,
    /// The ContractFolderStatus XML subtree as a JSON string
    pub contract_folder_status: Option<String>,
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
    /// Unknown values default to `PublicTenders`.
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
}
