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
            AppError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            AppError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            AppError::UrlError(msg) => write!(f, "Invalid URL: {}", msg),
            AppError::RegexError(msg) => write!(f, "Regex error: {}", msg),
            AppError::SelectorError(msg) => write!(f, "CSS selector error: {}", msg),
            AppError::PeriodValidationError { period, available } => {
                write!(f, "Period '{}' is not available. Available periods: {}", period, available)
            }
            AppError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            AppError::IoError(msg) => write!(f, "IO error: {}", msg),
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
