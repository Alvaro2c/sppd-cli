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
        let lower = value.to_lowercase();
        if MINOR_CONTRACTS_ALIASES.contains(&lower.as_str()) {
            Self::MinorContracts
        } else if PUBLIC_TENDERS_ALIASES.contains(&lower.as_str()) {
            Self::PublicTenders
        } else {
            eprintln!(
                "Unknown procurement type '{}', defaulting to 'public-tenders'",
                value
            );
            Self::PublicTenders
        }
    }
}
