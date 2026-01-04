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

/// Parses a period string into (year, month_opt) format.
///
/// Returns `Some((year, month_opt))` where:
/// - For YYYY format (4 digits): `month_opt` is `None`
/// - For YYYYMM format (6 digits): `month_opt` is `Some(1..=12)`
///
/// Returns `None` if the period format is invalid.
pub(crate) fn parse_period(period: &str) -> Option<(u32, Option<u32>)> {
    match period.len() {
        4 => period.parse().ok().map(|y| (y, None)),
        6 => {
            let year: u32 = period[..4].parse().ok()?;
            let month: u32 = period[4..].parse().ok()?;
            if (1..=12).contains(&month) {
                Some((year, Some(month)))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Compares two periods, handling YYYY vs YYYYMM formats correctly.
///
/// Returns `Some(Ordering)` if both periods are valid, `None` otherwise.
/// For YYYY format periods, they are considered to represent the entire year.
pub(crate) fn period_compare(period1: &str, period2: &str) -> Option<std::cmp::Ordering> {
    let (y1, m1) = parse_period(period1)?;
    let (y2, m2) = parse_period(period2)?;

    match y1.cmp(&y2) {
        std::cmp::Ordering::Equal => {
            match (m1, m2) {
                (None, None) => Some(std::cmp::Ordering::Equal),
                (None, Some(_)) => Some(std::cmp::Ordering::Less), // YYYY < any YYYYMM in same year
                (Some(_), None) => Some(std::cmp::Ordering::Greater), // YYYYMM > YYYY in same year
                (Some(m1), Some(m2)) => Some(m1.cmp(&m2)),
            }
        }
        ord => Some(ord),
    }
}

/// Checks if a period is within the specified range, handling YYYY vs YYYYMM formats.
///
/// For YYYY format boundaries:
/// - Start "2023" matches all periods >= 202301
/// - End "2023" matches all periods <= 202312
fn period_in_range(period: &str, start: Option<&str>, end: Option<&str>) -> bool {
    let (p_year, p_month) = match parse_period(period) {
        Some(parsed) => parsed,
        None => return false, // Invalid period format, skip it
    };

    // Check start boundary
    if let Some(start_period) = start {
        match parse_period(start_period) {
            Some((s_year, s_month_opt)) => {
                match p_year.cmp(&s_year) {
                    std::cmp::Ordering::Less => return false,
                    std::cmp::Ordering::Greater => {
                        // Period is in a later year than the start; continue checking end boundary
                    }
                    std::cmp::Ordering::Equal => {
                        // Same year, check month
                        if let Some(s_month) = s_month_opt {
                            // Start is YYYYMM, period must be >= start month
                            if let Some(p_month_val) = p_month {
                                if p_month_val < s_month {
                                    return false;
                                }
                            } else {
                                // Period is YYYY, start is YYYYMM - YYYY is less specific, so it's not >= YYYYMM
                                return false;
                            }
                        } else {
                            // Start is YYYY, matches all months in that year
                            // So if period is in same year, it matches (continue)
                        }
                    }
                }
            }
            None => return false, // Invalid start period
        }
    }

    // Check end boundary
    if let Some(end_period) = end {
        match parse_period(end_period) {
            Some((e_year, e_month_opt)) => {
                match p_year.cmp(&e_year) {
                    std::cmp::Ordering::Greater => {
                        // Period is in a later year
                        // If end is YYYY format, only match periods in that exact year
                        if e_month_opt.is_none() {
                            return false; // End is YYYY, period is in later year, don't match
                        }
                        // End is YYYYMM, period is in later year, so it doesn't match
                        return false;
                    }
                    std::cmp::Ordering::Less => {} // Continue, it's in range
                    std::cmp::Ordering::Equal => {
                        // Same year, check month
                        if let Some(e_month) = e_month_opt {
                            // End is YYYYMM, period must be <= end month
                            if let Some(p_month_val) = p_month {
                                if p_month_val > e_month {
                                    return false;
                                }
                            } else {
                                // Period is YYYY, end is YYYYMM - YYYY is not <= YYYYMM
                                return false;
                            }
                        } else {
                            // End is YYYY, matches all months in that year
                            // So if period is in same year, it matches (continue)
                        }
                    }
                }
            }
            None => return false, // Invalid end period
        }
    }

    true
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
    let mut filtered = BTreeMap::new();

    // Get sorted list of available periods as owned Strings (deterministic order)
    // BTreeMap keys are already ordered deterministically
    let available_periods: Vec<String> = links.keys().cloned().collect();
    let available_str = available_periods.join(", ");

    // Validate that specified periods have correct format and exist in links
    let validate_period = |period: Option<&str>| -> AppResult<()> {
        if let Some(p) = period {
            // First validate the format
            validate_period_format(p)?;
            // Then check if it exists exactly in links (no transformation)
            if !links.contains_key(p) {
                return Err(AppError::PeriodValidationError {
                    period: p.to_string(),
                    available: available_str.clone(),
                });
            }
        }
        Ok(())
    };

    validate_period(start_period)?;
    validate_period(end_period)?;

    // Validate that start <= end (if both are provided)
    if let (Some(start), Some(end)) = (start_period, end_period) {
        if let Some(ordering) = period_compare(start, end) {
            if ordering == std::cmp::Ordering::Greater {
                return Err(AppError::InvalidInput(format!(
                    "Start period '{start}' must be less than or equal to end period '{end}'"
                )));
            }
        }
    }

    // Filter periods using proper comparison logic
    for (period, url) in links.iter() {
        if period_in_range(period, start_period, end_period) {
            filtered.insert(period.to_owned(), url.to_owned());
        }
    }

    Ok(filtered)
}

#[cfg(test)]
mod tests {
    use super::{filter_periods_by_range, period_compare, validate_period_format};
    use crate::errors::AppError;
    use std::cmp::Ordering;
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
        assert_eq!(filtered.len(), 4); // 2023, 202301, 202302, 202303
        assert!(filtered.contains_key("2023"));
        assert!(filtered.contains_key("202301"));
        assert!(filtered.contains_key("202303"));
        assert!(!filtered.contains_key("202401"));
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

        // Filter with YYYY end - should include "2023" itself and all 2023XX periods
        let result = filter_periods_by_range(&links, None, Some("2023"));
        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 3); // 2023, 202301, 202312
        assert!(filtered.contains_key("2023"));
        assert!(filtered.contains_key("202301"));
        assert!(filtered.contains_key("202312"));
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

        // Filter with YYYY start and end - should include "2023" itself and all 2023XX periods
        let result = filter_periods_by_range(&links, Some("2023"), Some("2023"));
        assert!(result.is_ok());
        let filtered = result.unwrap();
        assert_eq!(filtered.len(), 3); // 2023, 202301, 202312
        assert!(filtered.contains_key("2023"));
        assert!(filtered.contains_key("202301"));
        assert!(filtered.contains_key("202312"));
        assert!(!filtered.contains_key("202212"));
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
    fn test_period_compare_yyyy_vs_yyyymm() {
        // YYYY < YYYYMM in same year
        assert_eq!(period_compare("2023", "202301"), Some(Ordering::Less));
        // YYYYMM > YYYY in same year
        assert_eq!(period_compare("202301", "2023"), Some(Ordering::Greater));
        // YYYY == YYYY
        assert_eq!(period_compare("2023", "2023"), Some(Ordering::Equal));
        // YYYYMM < YYYYMM (different months)
        assert_eq!(period_compare("202301", "202302"), Some(Ordering::Less));
        // Different years
        assert_eq!(period_compare("2022", "2023"), Some(Ordering::Less));
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
