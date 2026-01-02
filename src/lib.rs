//! sppd-cli library
//!
//! This crate provides the core functionality for the `sppd-cli` binary.
//!
//! ## Modules
//!
//! - [`downloader`] - Fetches ZIP file links and downloads archives from Spanish procurement data sources
//! - [`extractor`] - Extracts ZIP files containing XML/Atom feeds
//! - [`parser`] - Parses XML/Atom files and converts them to Parquet format
//! - [`cli`] - Command-line interface for orchestrating the download and processing workflow
//! - [`models`] - Data structures representing procurement entries and types
//! - [`errors`] - Error types used throughout the application

pub mod cli;
pub mod config;
pub mod constants;
pub mod downloader;
pub mod errors;
pub mod extractor;
pub mod models;
pub mod parser;
pub mod ui;
