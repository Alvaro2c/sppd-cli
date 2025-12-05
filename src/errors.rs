use std::fmt;

#[derive(Debug)]
#[allow(dead_code)]
pub enum AppError {
    /// Network request failed
    NetworkError(String),
    /// Failed to parse HTML/XML content
    ParseError(String),
    /// Invalid URL format
    UrlError(String),
    /// Regex compilation failed
    RegexError(String),
    /// Selector parsing failed
    SelectorError(String),
    /// Period validation failed
    PeriodValidationError { period: String, available: String },
    /// Invalid input format
    InvalidInput(String),
    /// IO operation failed
    IoError(String),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::NetworkError(msg) => write!(f, "Network error: {msg}"),
            AppError::ParseError(msg) => write!(f, "Parse error: {msg}"),
            AppError::UrlError(msg) => write!(f, "Invalid URL: {msg}"),
            AppError::RegexError(msg) => write!(f, "Regex error: {msg}"),
            AppError::SelectorError(msg) => write!(f, "CSS selector error: {msg}"),
            AppError::PeriodValidationError { period, available } => {
                write!(
                    f,
                    "Period '{period}' is not available. Available periods: {available}"
                )
            }
            AppError::InvalidInput(msg) => write!(f, "Invalid input: {msg}"),
            AppError::IoError(msg) => write!(f, "IO error: {msg}"),
        }
    }
}

impl std::error::Error for AppError {}

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

// Custom type alias for Results in this application
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
