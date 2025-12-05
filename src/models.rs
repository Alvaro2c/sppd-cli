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


#[cfg(test)]
mod tests {
    use super::ProcurementType;

    #[test]
    fn test_procurement_type_minor_contracts_primary_alias() {
        let proc_type = ProcurementType::from("minor-contracts");
        assert!(matches!(proc_type, ProcurementType::MinorContracts));
    }

    #[test]
    fn test_procurement_type_minor_contracts_short_alias() {
        let proc_type = ProcurementType::from("mc");
        assert!(matches!(proc_type, ProcurementType::MinorContracts));
    }

    #[test]
    fn test_procurement_type_minor_contracts_min_alias() {
        let proc_type = ProcurementType::from("min");
        assert!(matches!(proc_type, ProcurementType::MinorContracts));
    }

    #[test]
    fn test_procurement_type_public_tenders_primary_alias() {
        let proc_type = ProcurementType::from("public-tenders");
        assert!(matches!(proc_type, ProcurementType::PublicTenders));
    }

    #[test]
    fn test_procurement_type_public_tenders_short_alias() {
        let proc_type = ProcurementType::from("pt");
        assert!(matches!(proc_type, ProcurementType::PublicTenders));
    }

    #[test]
    fn test_procurement_type_public_tenders_pub_alias() {
        let proc_type = ProcurementType::from("pub");
        assert!(matches!(proc_type, ProcurementType::PublicTenders));
    }

    #[test]
    fn test_procurement_type_case_insensitive() {
        let proc_type = ProcurementType::from("MINOR-CONTRACTS");
        assert!(matches!(proc_type, ProcurementType::MinorContracts));

        let proc_type = ProcurementType::from("Public-Tenders");
        assert!(matches!(proc_type, ProcurementType::PublicTenders));
    }

    #[test]
    fn test_procurement_type_unknown_defaults_to_public_tenders() {
        let proc_type = ProcurementType::from("unknown-type");
        assert!(matches!(proc_type, ProcurementType::PublicTenders));
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
        assert!(matches!(proc_type, ProcurementType::PublicTenders));
    }

    #[test]
    fn test_procurement_type_whitespace() {
        let proc_type = ProcurementType::from("   ");
        assert!(matches!(proc_type, ProcurementType::PublicTenders));
    }
}
