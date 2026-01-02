//! sppd-cli library
//!
//! This crate provides the core functionality for the `sppd-cli` binary.
//! Keep the crate root minimal — implementation and tests live in their modules.
//!
//! ## Overview
//!
//! The library is organized into modules that handle different aspects of the procurement data pipeline:
//!
//! - [`downloader`] - Fetches ZIP file links and downloads archives from Spanish procurement data sources
//! - [`extractor`] - Extracts ZIP files containing XML/Atom feeds
//! - [`parser`] - Parses XML/Atom files and converts them to Parquet format
//! - [`cli`] - Command-line interface for orchestrating the download and processing workflow
//! - [`models`] - Data structures representing procurement entries and types
//! - [`errors`] - Error types used throughout the application
//!
//! ## Example Usage
//!
//! The typical workflow involves fetching available links, filtering by period range,
//! downloading files, extracting them, parsing XML content, and cleaning up temporary files:
//!
//! ```no_run
//! use sppd_cli::{downloader, cli, errors::AppResult};
//!
//! # async fn example() -> AppResult<()> {
//! // Fetch all available download links
//! let (minor_contracts_links, public_tenders_links) = downloader::fetch_all_links().await?;
//!
//! // Process CLI commands and execute downloads
//! cli::cli(&minor_contracts_links, &public_tenders_links).await?;
//! # Ok(())
//! # }
//! ```

pub mod cli;
pub mod config;
pub mod constants;
pub mod downloader;
pub mod errors;
pub mod extractor;
pub mod models;
pub mod parser;
pub mod ui;
