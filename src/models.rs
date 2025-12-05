use crate::constants::*;

pub enum ProcurementType {
    MinorContracts,
    PublicTenders,
}

impl ProcurementType {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::MinorContracts => "Minor Contracts",
            Self::PublicTenders => "Public Tenders",
        }
    }
}

impl From<&str> for ProcurementType {
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
