use crate::constants::*;
use serde::{Deserialize, Serialize};

/// Represents a single entry element from an XML/Atom feed.
///
/// This structure corresponds to an `<entry>` element in Atom feeds or similar
/// structures in XML files downloaded from Spanish procurement data sources.
/// All fields are optional to handle variations in the source data format.
///
/// # Example
///
/// ```
/// use sppd_cli::models::Entry;
///
/// let entry = Entry {
///     id: Some("12345".to_string()),
///     title: Some("Public Tender for IT Services".to_string()),
///     link: Some("https://example.com/tender/12345".to_string()),
///     summary: Some("Procurement of IT services for government agency".to_string()),
///     updated: Some("2023-01-15T10:30:00Z".to_string()),
///     contract_folder_status: Some("Active".to_string()),
/// };
/// ```
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
    ///
    /// # Example
    ///
    /// ```
    /// use sppd_cli::models::ProcurementType;
    ///
    /// assert_eq!(ProcurementType::MinorContracts.display_name(), "Minor Contracts");
    /// assert_eq!(ProcurementType::PublicTenders.display_name(), "Public Tenders");
    /// ```
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::MinorContracts => "Minor Contracts",
            Self::PublicTenders => "Public Tenders",
        }
    }
    /// Returns the download directory path for the procurement type (for ZIP downloads).
    ///
    /// # Example
    ///
    /// ```
    /// use sppd_cli::models::ProcurementType;
    ///
    /// assert_eq!(ProcurementType::MinorContracts.download_dir(), "data/tmp/mc");
    /// assert_eq!(ProcurementType::PublicTenders.download_dir(), "data/tmp/pt");
    /// ```
    pub fn download_dir(&self) -> &'static str {
        match self {
            Self::MinorContracts => "data/tmp/mc",
            Self::PublicTenders => "data/tmp/pt",
        }
    }

    /// Returns the extraction directory path for the procurement type (for XML extraction).
    ///
    /// # Example
    ///
    /// ```
    /// use sppd_cli::models::ProcurementType;
    ///
    /// assert_eq!(ProcurementType::MinorContracts.extract_dir(), "data/tmp/mc");
    /// assert_eq!(ProcurementType::PublicTenders.extract_dir(), "data/tmp/pt");
    /// ```
    pub fn extract_dir(&self) -> &'static str {
        match self {
            Self::MinorContracts => "data/tmp/mc",
            Self::PublicTenders => "data/tmp/pt",
        }
    }

    /// Returns the directory path for the final parquet files.
    ///
    /// # Example
    ///
    /// ```
    /// use sppd_cli::models::ProcurementType;
    ///
    /// assert_eq!(ProcurementType::MinorContracts.parquet_dir(), "data/parquet/mc");
    /// assert_eq!(ProcurementType::PublicTenders.parquet_dir(), "data/parquet/pt");
    /// ```
    pub fn parquet_dir(&self) -> &'static str {
        match self {
            Self::MinorContracts => "data/parquet/mc",
            Self::PublicTenders => "data/parquet/pt",
        }
    }
}

impl From<&str> for ProcurementType {
    /// Converts a string to a `ProcurementType`, accepting various aliases.
    ///
    /// This conversion is case-insensitive and trims whitespace. It's used by the CLI
    /// to parse the `--type` argument.
    ///
    /// # Accepted Aliases
    ///
    /// **Minor Contracts:**
    /// - `"mc"`
    /// - `"minor-contracts"`
    /// - `"min"`
    ///
    /// **Public Tenders:**
    /// - `"pt"`
    /// - `"pub"`
    /// - `"public-tenders"`
    ///
    /// # Default Behavior
    ///
    /// If the input doesn't match any known alias, it defaults to `PublicTenders`.
    /// This matches the CLI's default behavior.
    ///
    /// # Example
    ///
    /// ```
    /// use sppd_cli::models::ProcurementType;
    ///
    /// // Minor contracts aliases
    /// assert_eq!(ProcurementType::from("mc"), ProcurementType::MinorContracts);
    /// assert_eq!(ProcurementType::from("MC"), ProcurementType::MinorContracts);
    /// assert_eq!(ProcurementType::from("minor-contracts"), ProcurementType::MinorContracts);
    /// assert_eq!(ProcurementType::from("min"), ProcurementType::MinorContracts);
    ///
    /// // Public tenders aliases
    /// assert_eq!(ProcurementType::from("pt"), ProcurementType::PublicTenders);
    /// assert_eq!(ProcurementType::from("PT"), ProcurementType::PublicTenders);
    /// assert_eq!(ProcurementType::from("public-tenders"), ProcurementType::PublicTenders);
    /// assert_eq!(ProcurementType::from("pub"), ProcurementType::PublicTenders);
    ///
    /// // Unknown values default to PublicTenders
    /// assert_eq!(ProcurementType::from("unknown"), ProcurementType::PublicTenders);
    /// assert_eq!(ProcurementType::from(""), ProcurementType::PublicTenders);
    /// ```
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
