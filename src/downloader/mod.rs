//! Download and fetch operations for procurement data.
//!
//! This module provides functions to fetch ZIP file links from Spanish procurement data sources
//! and download the archives for processing. The main entry points are [`fetch_all_links`] and [`download_files`].

mod file_downloader;
mod link_fetcher;
mod period_filter;

// Re-export public API
pub use file_downloader::download_files;
pub use link_fetcher::{fetch_all_links, fetch_zip, parse_zip_links};
pub use period_filter::{filter_periods_by_range, validate_period_format};
