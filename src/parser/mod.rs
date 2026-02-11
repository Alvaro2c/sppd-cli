//! XML parsing and Parquet output operations.
//!
//! This module extracts, parses, and transforms XML/Atom files into Parquet format. It handles
//! ZIP extraction, XML parsing into `Entry` structures, and writing to Parquet files.
//! Main entry points are [`find_xmls`] and [`parse_xmls`].

mod cleanup;
mod contract_folder_status;
mod file_finder;
mod parquet_writer;
mod scope;
mod xml_parser;

// Re-export public API
pub use cleanup::cleanup_files;
pub use file_finder::find_xmls;
pub use parquet_writer::parse_xmls;
