# SPPD CLI

A command-line tool for downloading, extracting, and converting Spanish public procurement data (SPPD) to Parquet format.

## Requirements

- Rust 1.56 or later

## Features

- Downloads ZIP archives from Spanish procurement data sources
- Extracts and parses XML/ATOM files
- Converts data to Parquet format for analysis
- Supports both minor contracts and public tenders

## Usage

Download, extract, and convert procurement data:

```bash
cargo run -- download [OPTIONS]
```

### Options

- `-t, --type <TYPE>`: Procurement type (default: `public-tenders`)
  - `public-tenders` (aliases: `pt`, `pub`)
  - `minor-contracts` (aliases: `mc`, `min`)
- `-s, --start <PERIOD>`: Start period (format: `YYYY` or `YYYYMM`)
- `-e, --end <PERIOD>`: End period (format: `YYYY` or `YYYYMM`)
- Available periods are:
  - Previous years: full years only (`YYYY`)
  - Current year: all months up to the download date (`YYYYMM`)

### Examples

```bash
# Download all available public tenders
cargo run -- download

# Download public tenders for 2023
cargo run -- download -t public-tenders -s 2023 -e 2023

# Download minor contracts for January 2025
cargo run -- download -t mc -s 202501 -e 202501
```

### Output

- ZIP files: `data/tmp/{mc,pt}/`
- Parquet files: `data/parquet/{mc,pt}/`

## Logging

Control log levels with `RUST_LOG`:

```bash
RUST_LOG=debug cargo run -- download  # Detailed output
RUST_LOG=warn cargo run -- download   # Warnings and errors only
```
