# SPPD CLI

A command-line tool for downloading, extracting, and converting Spanish public procurement data to Parquet format.

## Installation

### Prerequisites

- Rust 1.56 or later

### Build from Source

```bash
git clone https://github.com/Alvaro2c/sppd-cli.git
cd sppd-cli
cargo build --release
```

The binary will be available at `target/release/sppd-cli`.

## Usage

### Basic Command

```bash
cargo run -- download [OPTIONS]
```

### Options

- `-t, --type <TYPE>`: Procurement type (default: `public-tenders`)
  - `public-tenders` (aliases: `pt`, `pub`)
  - `minor-contracts` (aliases: `mc`, `min`)
- `-s, --start <PERIOD>`: Start period (format: `YYYY` or `YYYYMM`)
- `-e, --end <PERIOD>`: End period (format: `YYYY` or `YYYYMM`)
- `--cleanup <yes|no>`: Delete intermediate files (ZIP and XML/Atom) after processing, keeping only Parquet files (default: `yes`)
- `--batch-size <SIZE>`: Number of XML files to process per batch (default: 100). Can also be set via `SPPD_BATCH_SIZE` env var.

**Available periods:**
- Previous years: full years only (`YYYY`)
- Current year: all months up to the download date (`YYYYMM`)

### Environment Variables

- `SPPD_BATCH_SIZE`: XML files per batch (overrides default, not CLI)
- `RUST_LOG`: Log level (`debug`, `info`, `warn`)

### Examples

```bash
# Download all available public tenders
cargo run -- download

# Download public tenders for 2023
cargo run -- download -t public-tenders -s 2023 -e 2023

# Download minor contracts for January 2025
cargo run -- download -t mc -s 202501 -e 202501

# Keep intermediate files (don't cleanup)
cargo run -- download --cleanup no
```

### Output

- ZIP files: `data/tmp/{mc,pt}/`
- Parquet files: `data/parquet/{mc,pt}/`

### Logging

Control log levels with `RUST_LOG`:

```bash
RUST_LOG=debug cargo run -- download  # Detailed output
RUST_LOG=warn cargo run -- download   # Warnings and errors only
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is dual-licensed under either:

- Apache License, Version 2.0 ([LICENSE](LICENSE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT License (http://opensource.org/licenses/MIT)

at your option.
