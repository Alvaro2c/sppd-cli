mod file_downloader;
mod link_fetcher;
mod period_filter;

// Re-export public API
pub use file_downloader::download_files;
pub use link_fetcher::{fetch_all_links, fetch_zip, parse_zip_links};
pub use period_filter::{filter_periods_by_range, validate_period_format};
