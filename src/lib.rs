//! sppd-cli library
//!
//! This crate provides the core functionality for the `sppd-cli` binary.
//!
//! ## Modules
//!
//! - [`downloader`] - Fetches ZIP file links and downloads archives from Spanish procurement data sources
//! - [`extractor`] - Extracts ZIP files containing XML/Atom feeds
//! - [`parser`] - Parses XML/Atom files and converts them to Parquet format (see [`models::Entry`] for the output schema documentation)
//! - [`cli`] - Command-line interface for orchestrating the download and processing workflow
//! - [`models`] - Data structures representing procurement entries and types (each `Entry` mirrors the Parquet output schema)
//! - [`errors`] - Error types used throughout the application
//! - [`config`] - Configuration types and helpers for pipeline defaults and TOML loading
//!
//! For detailed usage, examples, and the full output schema (13–14 Parquet columns), see the [repository README](https://github.com/Alvaro2c/sppd-cli).

pub mod cli;
pub mod config;
pub mod downloader;
pub mod errors;
pub mod extractor;
pub mod models;
pub mod parser;
mod utils;
