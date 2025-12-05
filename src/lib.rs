// Library module definitions for testing and reusability
pub mod constants;
pub mod cli;
pub mod downloader;
pub mod models;
pub mod errors;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use crate::models::ProcurementType;
    use crate::downloader::filter_periods_by_range;
    use crate::errors::AppError;

    // ===== ProcurementType Tests =====

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

    // ===== Period Filtering Tests =====

    fn create_test_links() -> BTreeMap<String, String> {
        let mut links = BTreeMap::new();
        links.insert("202301".to_string(), "https://example.com/202301.zip".to_string());
        links.insert("202302".to_string(), "https://example.com/202302.zip".to_string());
        links.insert("202303".to_string(), "https://example.com/202303.zip".to_string());
        links.insert("202304".to_string(), "https://example.com/202304.zip".to_string());
        links.insert("202305".to_string(), "https://example.com/202305.zip".to_string());
        links
    }

    #[test]
    fn test_filter_all_periods_no_constraints() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, None, None);

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 5);
    }

    #[test]
    fn test_filter_with_start_period_only() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, Some("202303"), None);

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 3); // 202303, 202304, 202305
        assert!(filtered.contains_key("202303"));
        assert!(filtered.contains_key("202305"));
        assert!(!filtered.contains_key("202302"));
    }

    #[test]
    fn test_filter_with_end_period_only() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, None, Some("202303"));

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 3); // 202301, 202302, 202303
        assert!(filtered.contains_key("202301"));
        assert!(filtered.contains_key("202303"));
        assert!(!filtered.contains_key("202304"));
    }

    #[test]
    fn test_filter_with_start_and_end_period() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, Some("202302"), Some("202304"));

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 3); // 202302, 202303, 202304
        assert!(filtered.contains_key("202302"));
        assert!(filtered.contains_key("202303"));
        assert!(filtered.contains_key("202304"));
        assert!(!filtered.contains_key("202301"));
        assert!(!filtered.contains_key("202305"));
    }

    #[test]
    fn test_filter_single_period() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, Some("202303"), Some("202303"));

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 1);
        assert!(filtered.contains_key("202303"));
    }

    #[test]
    fn test_filter_invalid_start_period() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, Some("999999"), None);

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::PeriodValidationError { period, .. } => {
                assert_eq!(period, "999999");
            }
            _ => panic!("Expected PeriodValidationError"),
        }
    }

    #[test]
    fn test_filter_invalid_end_period() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, None, Some("999999"));

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::PeriodValidationError { period, .. } => {
                assert_eq!(period, "999999");
            }
            _ => panic!("Expected PeriodValidationError"),
        }
    }

    #[test]
    fn test_filter_both_periods_invalid() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, Some("999999"), Some("888888"));

        // Should fail on the first invalid period (start)
        assert!(result.is_err());
    }

    #[test]
    fn test_filter_error_includes_available_periods() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, Some("999999"), None);

        assert!(result.is_err());
        if let AppError::PeriodValidationError { available, .. } = result.unwrap_err() {
            // Available periods should be comma-separated and sorted
            assert!(available.contains("202301"));
            assert!(available.contains("202305"));
        } else {
            panic!("Expected PeriodValidationError");
        }
    }

    #[test]
    fn test_filter_empty_hash_map() {
        let links = BTreeMap::new();
        let result = filter_periods_by_range(&links, None, None);

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_filter_preserves_urls() {
        let mut links = BTreeMap::new();
        let url1 = "https://example.com/202301.zip".to_string();
        let url2 = "https://example.com/202302.zip".to_string();
        links.insert("202301".to_string(), url1.clone());
        links.insert("202302".to_string(), url2.clone());

        let result = filter_periods_by_range(&links, None, None);
        let filtered = result.unwrap();

        assert_eq!(filtered.get("202301"), Some(&url1));
        assert_eq!(filtered.get("202302"), Some(&url2));
    }

    // ===== Error Type Tests =====

    #[test]
    fn test_period_validation_error_display() {
        let err = AppError::PeriodValidationError {
            period: "202301".to_string(),
            available: "202302, 202303".to_string(),
        };

        let error_msg = err.to_string();
        assert!(error_msg.contains("202301"));
        assert!(error_msg.contains("202302"));
        assert!(error_msg.contains("202303"));
    }

    #[test]
    fn test_network_error_display() {
        let err = AppError::NetworkError("Connection timeout".to_string());
        assert!(err.to_string().contains("Network error"));
        assert!(err.to_string().contains("Connection timeout"));
    }

    #[test]
    fn test_url_error_display() {
        let err = AppError::UrlError("Invalid URL format".to_string());
        assert!(err.to_string().contains("Invalid URL"));
        assert!(err.to_string().contains("Invalid URL format"));
    }

    #[test]
    fn test_regex_error_display() {
        let err = AppError::RegexError("Invalid regex".to_string());
        assert!(err.to_string().contains("Regex error"));
    }

    #[test]
    fn test_selector_error_display() {
        let err = AppError::SelectorError("Invalid selector".to_string());
        assert!(err.to_string().contains("CSS selector error"));
    }

    #[test]
    fn test_invalid_input_error_display() {
        let err = AppError::InvalidInput("Not a number".to_string());
        assert!(err.to_string().contains("Invalid input"));
    }

    #[test]
    fn test_app_error_implements_error_trait() {
        use std::error::Error;
        let err: Box<dyn Error> = Box::new(AppError::NetworkError("test".to_string()));
        assert!(!err.to_string().is_empty());
    }

    // ===== Edge Case Tests =====

    #[test]
    fn test_filter_with_non_numeric_periods() {
        let mut links = BTreeMap::new();
        links.insert("invalid".to_string(), "https://example.com/invalid.zip".to_string());
        links.insert("202301".to_string(), "https://example.com/202301.zip".to_string());

        let result = filter_periods_by_range(&links, None, None);
        assert!(result.is_ok());
        let filtered = result.unwrap();

        // Non-numeric periods are silently skipped
        assert_eq!(filtered.len(), 1);
        assert!(filtered.contains_key("202301"));
    }

    #[test]
    fn test_filter_start_greater_than_end() {
        let links = create_test_links();
        // This should return empty because no periods fall in the range
        let result = filter_periods_by_range(&links, Some("202305"), Some("202301"));

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_procurement_type_empty_string() {
        let proc_type = ProcurementType::from("");
        // Empty string doesn't match any alias, so defaults to PublicTenders
        assert!(matches!(proc_type, ProcurementType::PublicTenders));
    }

    #[test]
    fn test_procurement_type_whitespace() {
        let proc_type = ProcurementType::from("   ");
        // Whitespace after lowercase doesn't match aliases
        assert!(matches!(proc_type, ProcurementType::PublicTenders));
    }
}
