use crate::errors::{AppError, AppResult};
use std::collections::BTreeMap;

/// Validates that a period string matches the expected format (YYYY or YYYYMM).
///
/// Checks that the period contains only ASCII digits and has exactly 4 digits (YYYY) or 6 digits (YYYYMM).
///
/// Returns `Ok(())` if valid, or `InvalidInput` error otherwise.
pub fn validate_period_format(period: &str) -> AppResult<()> {
    if period.is_empty() {
        return Err(AppError::InvalidInput(
            "Period must be YYYY or YYYYMM format (4 or 6 digits), got empty string".to_string(),
        ));
    }
    if !period.chars().all(|c| c.is_ascii_digit()) {
        return Err(AppError::InvalidInput(format!(
            "Period must contain only digits, got: {period}"
        )));
    }
    match period.len() {
        4 | 6 => Ok(()),
        _ => Err(AppError::InvalidInput(format!(
            "Period must be YYYY or YYYYMM format (4 or 6 digits), got: {} ({} digits)",
            period,
            period.len()
        ))),
    }
}

/// Filters links by period range, validating that specified periods exist.
///
/// This function filters a map of period-to-URL links based on a start and/or end period.
/// Periods are compared correctly, handling both YYYY and YYYYMM formats. The range is inclusive
/// on both ends.
///
/// # Arguments
///
/// * `links` - Map of period strings to URLs to filter
/// * `start_period` - Optional start period (inclusive). If `None`, no lower bound.
/// * `end_period` - Optional end period (inclusive). If `None`, no upper bound.
///
/// # Returns
///
/// A filtered map containing only periods within the specified range.
///
/// # Errors
///
/// Returns `InvalidInput` if `start_period` or `end_period` has an invalid format
/// (not YYYY or YYYYMM). Returns `PeriodValidationError` if the period format is valid
/// but doesn't exist in the `links` map.
///
pub fn filter_periods_by_range(
    links: &BTreeMap<String, String>,
    start_period: Option<&str>,
    end_period: Option<&str>,
) -> AppResult<BTreeMap<String, String>> {
    let available_str = links.keys().cloned().collect::<Vec<_>>().join(", ");

    for period in [start_period, end_period].into_iter().flatten() {
        validate_period_format(period)?;
        if !links.contains_key(period) {
            return Err(AppError::PeriodValidationError {
                period: period.to_string(),
                available: available_str.clone(),
            });
        }
    }

    let start_key = start_period.map(|s| s.to_string());
    let end_key = end_period.map(|e| e.to_string());

    if let (Some(start), Some(end)) = (&start_key, &end_key) {
        if start > end {
            return Err(AppError::InvalidInput(format!(
                "Start period '{start}' must be less than or equal to end period '{end}'"
            )));
        }
    }

    let range_iter = match (&start_key, &end_key) {
        (Some(start), Some(end)) => links.range(start.clone()..=end.clone()),
        (Some(start), None) => links.range(start.clone()..),
        (None, Some(end)) => links.range(..=end.clone()),
        (None, None) => links.range::<String, _>(..),
    };

    let filtered = range_iter
        .filter(|(period, _)| validate_period_format(period).is_ok())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    Ok(filtered)
}

#[cfg(test)]
mod tests {
    use super::{filter_periods_by_range, validate_period_format};
    use crate::errors::AppError;
    use std::collections::BTreeMap;

    fn create_test_links() -> BTreeMap<String, String> {
        let mut links = BTreeMap::new();
        links.insert(
            "202301".to_string(),
            "https://example.com/202301.zip".to_string(),
        );
        links.insert(
            "202302".to_string(),
            "https://example.com/202302.zip".to_string(),
        );
        links.insert(
            "202303".to_string(),
            "https://example.com/202303.zip".to_string(),
        );
        links.insert(
            "202304".to_string(),
            "https://example.com/202304.zip".to_string(),
        );
        links.insert(
            "202305".to_string(),
            "https://example.com/202305.zip".to_string(),
        );
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

    #[test]
    fn test_filter_with_non_numeric_periods() {
        let mut links = BTreeMap::new();
        links.insert(
            "invalid".to_string(),
            "https://example.com/invalid.zip".to_string(),
        );
        links.insert(
            "202301".to_string(),
            "https://example.com/202301.zip".to_string(),
        );

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
        // This should return an error because start > end
        let result = filter_periods_by_range(&links, Some("202305"), Some("202301"));

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("Start period"));
                assert!(msg.contains("must be less than or equal to end period"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_filter_start_equal_to_end() {
        let links = create_test_links();
        // Start == end should be valid and return only that period
        let result = filter_periods_by_range(&links, Some("202303"), Some("202303"));

        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 1);
        assert!(filtered.contains_key("202303"));
    }

    #[test]
    fn test_filter_with_yyyy_format_start() {
        // Test filtering with YYYY format when links have both YYYY and YYYYMM formats
        let mut links = BTreeMap::new();
        links.insert(
            "2023".to_string(),
            "https://example.com/2023.zip".to_string(),
        );
        links.insert(
            "202301".to_string(),
            "https://example.com/202301.zip".to_string(),
        );
        links.insert(
            "202302".to_string(),
            "https://example.com/202302.zip".to_string(),
        );
        links.insert(
            "202303".to_string(),
            "https://example.com/202303.zip".to_string(),
        );
        links.insert(
            "202401".to_string(),
            "https://example.com/202401.zip".to_string(),
        );

        // Filter with YYYY start - should include "2023" itself and all 2023XX periods
        let result = filter_periods_by_range(&links, Some("2023"), None);
        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 5); // 2023, 202301, 202302, 202303, 202401
        assert!(filtered.contains_key("2023"));
        assert!(filtered.contains_key("202301"));
        assert!(filtered.contains_key("202303"));
        assert!(filtered.contains_key("202401"));
    }

    #[test]
    fn test_filter_with_yyyy_format_end() {
        // Test filtering with YYYY format end when links have both YYYY and YYYYMM formats
        let mut links = BTreeMap::new();
        links.insert(
            "2023".to_string(),
            "https://example.com/2023.zip".to_string(),
        );
        links.insert(
            "202301".to_string(),
            "https://example.com/202301.zip".to_string(),
        );
        links.insert(
            "202312".to_string(),
            "https://example.com/202312.zip".to_string(),
        );
        links.insert(
            "202401".to_string(),
            "https://example.com/202401.zip".to_string(),
        );

        // Filter with YYYY end - should include only "2023" because other entries are lexicographically greater
        let result = filter_periods_by_range(&links, None, Some("2023"));
        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 1);
        assert!(filtered.contains_key("2023"));
        assert!(!filtered.contains_key("202301"));
        assert!(!filtered.contains_key("202312"));
        assert!(!filtered.contains_key("202401"));
    }

    #[test]
    fn test_filter_with_yyyy_format_both() {
        // Test filtering with YYYY format for both start and end when links have both formats
        let mut links = BTreeMap::new();
        links.insert(
            "202212".to_string(),
            "https://example.com/202212.zip".to_string(),
        );
        links.insert(
            "2023".to_string(),
            "https://example.com/2023.zip".to_string(),
        );
        links.insert(
            "202301".to_string(),
            "https://example.com/202301.zip".to_string(),
        );
        links.insert(
            "202312".to_string(),
            "https://example.com/202312.zip".to_string(),
        );
        links.insert(
            "202401".to_string(),
            "https://example.com/202401.zip".to_string(),
        );

        // Filter with YYYY start and end - should include only "2023"
        let result = filter_periods_by_range(&links, Some("2023"), Some("2023"));
        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 1);
        assert!(filtered.contains_key("2023"));
        assert!(!filtered.contains_key("202212"));
        assert!(!filtered.contains_key("202301"));
        assert!(!filtered.contains_key("202312"));
        assert!(!filtered.contains_key("202401"));
    }

    #[test]
    fn test_filter_strict_validation_yyyy_not_in_links() {
        // Test that YYYY format period must exist exactly in links (no fallback to YYYYMM)
        let mut links = BTreeMap::new();
        links.insert(
            "202301".to_string(),
            "https://example.com/202301.zip".to_string(),
        );
        links.insert(
            "202302".to_string(),
            "https://example.com/202302.zip".to_string(),
        );

        // Trying to use "2023" when it doesn't exist in links should fail
        let result = filter_periods_by_range(&links, Some("2023"), None);
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::PeriodValidationError { period, .. } => {
                assert_eq!(period, "2023");
            }
            _ => panic!("Expected PeriodValidationError"),
        }
    }

    #[test]
    fn test_validate_period_format_valid_yyyy() {
        assert!(validate_period_format("2023").is_ok());
        assert!(validate_period_format("2024").is_ok());
        assert!(validate_period_format("1999").is_ok());
    }

    #[test]
    fn test_validate_period_format_valid_yyyymm() {
        assert!(validate_period_format("202301").is_ok());
        assert!(validate_period_format("202312").is_ok());
        assert!(validate_period_format("202401").is_ok());
    }

    #[test]
    fn test_validate_period_format_invalid_too_short() {
        let result = validate_period_format("202");
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("4 or 6 digits"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_period_format_invalid_too_long() {
        let result = validate_period_format("20230101");
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("4 or 6 digits"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_period_format_invalid_five_digits() {
        let result = validate_period_format("20231");
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("4 or 6 digits"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_period_format_invalid_non_numeric() {
        let result = validate_period_format("abcd");
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("only digits"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_period_format_invalid_mixed_chars() {
        let result = validate_period_format("2023ab");
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("only digits"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_validate_period_format_empty_string() {
        let result = validate_period_format("");
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("empty string"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_filter_periods_invalid_format_start() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, Some("abc"), None);

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("only digits"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_filter_periods_invalid_format_end() {
        let links = create_test_links();
        let result = filter_periods_by_range(&links, None, Some("20231")); // 5 digits

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::InvalidInput(msg) => {
                assert!(msg.contains("4 or 6 digits"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }
}
