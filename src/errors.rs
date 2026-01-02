/// Application error types for the SPPD CLI.
///
/// Represents all possible errors that can occur during the procurement data download and processing workflow.
/// Implements `From` traits for common error types, allowing automatic conversion using the `?` operator.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// Network request failed (e.g., HTTP errors, timeouts)
    #[error("Network error: {0}")]
    NetworkError(String),
    /// Failed to parse HTML/XML content
    #[error("Parse error: {0}")]
    ParseError(String),
    /// Invalid URL format
    #[error("Invalid URL: {0}")]
    UrlError(String),
    /// Regex compilation failed
    #[error("Regex error: {0}")]
    RegexError(String),
    /// CSS selector parsing failed
    #[error("CSS selector error: {0}")]
    SelectorError(String),
    /// Period validation failed (requested period not available)
    #[error("Period '{period}' is not available. Available periods: {available}")]
    PeriodValidationError { period: String, available: String },
    /// Invalid input format (e.g., malformed data)
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    /// IO operation failed (e.g., file read/write errors)
    #[error("IO error: {0}")]
    IoError(String),
}

// Conversion implementations for common errors
impl From<reqwest::Error> for AppError {
    fn from(err: reqwest::Error) -> Self {
        AppError::NetworkError(err.to_string())
    }
}

impl From<url::ParseError> for AppError {
    fn from(err: url::ParseError) -> Self {
        AppError::UrlError(err.to_string())
    }
}

impl From<regex::Error> for AppError {
    fn from(err: regex::Error) -> Self {
        AppError::RegexError(err.to_string())
    }
}

impl From<std::num::ParseIntError> for AppError {
    fn from(err: std::num::ParseIntError) -> Self {
        AppError::InvalidInput(err.to_string())
    }
}

impl From<std::io::Error> for AppError {
    fn from(err: std::io::Error) -> Self {
        AppError::IoError(err.to_string())
    }
}

impl From<quick_xml::Error> for AppError {
    fn from(err: quick_xml::Error) -> Self {
        AppError::ParseError(format!("XML parsing error: {err}"))
    }
}

/// Result type alias for application operations.
///
/// Convenience type alias for `Result<T, AppError>` used throughout the application.
pub type AppResult<T> = Result<T, AppError>;

#[cfg(test)]
mod tests {
    use super::AppError;

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
}
