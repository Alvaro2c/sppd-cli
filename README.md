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

### Manual CLI

```bash
cargo run -- cli [OPTIONS]
```

### Options

- `-t, --type <TYPE>`: Procurement type (default: `public-tenders`)
  - `public-tenders` (aliases: `pt`, `pub`)
  - `minor-contracts` (aliases: `mc`, `min`)
- `-s, --start <PERIOD>`: Start period (format: `YYYY` or `YYYYMM`)
- `-e, --end <PERIOD>`: End period (format: `YYYY` or `YYYYMM`)

Cleanup is always enabled for the manual CLI invocation. Use a TOML configuration file to change that behavior.

**Available periods:**
- Previous years: full years only (`YYYY`)
- Current year: all months up to the download date (`YYYYMM`)

### TOML Configuration

```bash
cargo run -- toml config/prod.toml
```

The TOML file lets you declare both the run parameters (`type`, `start`, `end`, `cleanup`) and the pipeline defaults (batch size, retry/backoff, directories, etc.). For example:

```toml
type = "public-tenders"
start = "202301"
end = "202312"
cleanup = false

batch_size = 100
concurrent_downloads = 4
retry_initial_delay_ms = 1000
retry_max_delay_ms = 10000

download_dir_mc = "data/tmp/mc"
download_dir_pt = "data/tmp/pt"
parquet_dir_mc = "data/parquet/mc"
parquet_dir_pt = "data/parquet/pt"
```

Only `type`, `start`, and `end` are required; the rest default to the built-in values.

### Environment Variables

- `RUST_LOG`: Log level (`debug`, `info`, `warn`)

### Examples

```bash
# Manual download (default cleanup, defaults for batch size, retries, etc.)
cargo run -- cli -t public-tenders -s 2023 -e 2023

# Run with a TOML configuration file (for automation/orchestration)
cargo run -- toml config/prod.toml
```

### Output

- ZIP files: `data/tmp/{mc,pt}/`
- Parquet files: `data/parquet/{mc,pt}/`

### Logging

Control log levels with `RUST_LOG`:

```bash
RUST_LOG=debug cargo run -- cli  # Detailed output
RUST_LOG=warn cargo run -- cli   # Warnings and errors only
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is dual-licensed under either:

- Apache License, Version 2.0 ([LICENSE](LICENSE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT License (http://opensource.org/licenses/MIT)

at your option.
