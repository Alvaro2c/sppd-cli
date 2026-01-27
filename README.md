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

## Documentation

Docs are published via GitHub Pages: https://alvaro2c.github.io/sppd-cli/sppd_cli/

## Library Usage

Add the crate directly from GitHub:

```toml
sppd-cli = { git = "https://github.com/Alvaro2c/sppd-cli" }
```

```rust
use sppd_cli::{downloader, extractor, parser};
```

## Architecture

```
Downloader -> Extractor -> Parser -> Parquet
    |             |           |
  fetch       unzip ZIP    parse XML
  ZIP links   archives     to DataFrame
```

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
- `-r, --read-concurrency <N>` (alias `--rc`): Number of XML files read concurrently during parsing (default: `16`)
- `-c, --concat-batches` (alias `--cb`): Merge per-batch Parquet files back into a single file per period

Cleanup is always enabled for the manual CLI invocation. Use a TOML configuration file to change that behavior.

**Available periods:**
- Previous years: full years only (`YYYY`)
- Current year: all months up to the download date (`YYYYMM`)

### TOML Configuration

```bash
cargo run -- toml config/prod.toml
```

The TOML file lets you declare the CLI run parameters and, optionally, any of the pipeline defaults. The parser fails if you omit any **required** field (`type`, `start`, or `end`) or include an unknown key (typos get rejected). Everything else uses the built-in defaults unless you override it.

Required keys:

- `type`: `public-tenders` (`pt`, `pub`) or `minor-contracts` (`mc`, `min`)
- `start`: period in `YYYY` or `YYYYMM`
- `end`: period in `YYYY` or `YYYYMM`

Optional overrides:

- `cleanup` (bool, defaults to `true`)
- Pipeline defaults:
  - `batch_size` (files per batch when parsing; default `150`; bounds the peak in-memory DataFrame)
  - `read_concurrency` (number of XML files read in parallel; default `16`)
  - `concat_batches` (bool, default `false`; merge per-batch parquet files into a single period file)
  - `max_retries` (default `3`)
  - `retry_initial_delay_ms` (default `1000`)
  - `retry_max_delay_ms` (default `10000`)
  - `concurrent_downloads` (default `4`)
  - `download_dir_mc`, `download_dir_pt`
  - `parquet_dir_mc`, `parquet_dir_pt`

Example:

```toml
type = "public-tenders"
start = "202501"
end = "202502"
cleanup = false

batch_size = 150
read_concurrency = 16
concat_batches = false
max_retries = 5
retry_initial_delay_ms = 1000
retry_max_delay_ms = 10000
concurrent_downloads = 4

download_dir_mc = "data/tmp/mc"
download_dir_pt = "data/tmp/pt"
parquet_dir_mc = "data/parquet/mc"
parquet_dir_pt = "data/parquet/pt"
```

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

### Output Schema

Each Parquet record mirrors an Atom `<entry>` plus the extracted `ContractFolderStatus` data, resulting in 47 columns:

| Column | Description |
|--------|-------------|
| `id` | Atom entry ID |
| `title` | Entry title |
| `link` | Entry link URL |
| `summary` | Entry summary text |
| `updated` | Last updated timestamp |
| `cfs_status_code` | `<cbc-place-ext:ContractFolderStatusCode>` |
| `cfs_id` | `<cbc:ContractFolderID>` |
| `cfs_project_name` | First `<cbc:Name>` inside `<cac:ProcurementProject>` |
| `cfs_project_type_code` | `<cac:ProcurementProject>/<cbc:TypeCode>` |
| `cfs_project_sub_type_code` | `<cac:ProcurementProject>/<cbc:SubTypeCode>` |
| `cfs_project_total_amount` | `<cac:ProcurementProject>/<cac:BudgetAmount>/<cbc:TotalAmount>` value |
| `cfs_project_total_currency` | `currencyID` attribute from the total amount |
| `cfs_project_tax_exclusive_amount` | `<cac:ProcurementProject>/<cac:BudgetAmount>/<cbc:TaxExclusiveAmount>` value |
| `cfs_project_tax_exclusive_currency` | `currencyID` attribute from the tax-exclusive amount |
| `cfs_project_cpv_codes` | `_`-joined `<cbc:ItemClassificationCode>` values |
| `cfs_project_country_code` | `<cac:RealizedLocation>/<cac:Address>/<cac:Country>/<cbc:IdentificationCode>` |
| `cfs_project_lots` | List of `<cac:ProcurementProjectLot>` structs, each containing `id`, `name`, budget amounts with currencies, `_`-concatenated `cpv_code`/`cpv_code_list_uri`, and country code/`country_code_list_uri` |
| `cfs_contracting_party_name` | `<cac:LocatedContractingParty>/<cac:Party>/<cac:PartyName>/<cbc:Name>` |
| `cfs_contracting_party_website` | `<cac:LocatedContractingParty>/<cac:Party>/<cbc:WebsiteURI>` |
| `cfs_contracting_party_type_code` | `<cac:LocatedContractingParty>/<cbc:ContractingPartyTypeCode>` |
| `cfs_contracting_party_id` | `<cac:LocatedContractingParty>/<cac:Party>/<cac:PartyIdentification>/<cbc:ID>` |
| `cfs_contracting_party_activity_code` | `<cac:LocatedContractingParty>/<cbc:ActivityCode>` |
| `cfs_contracting_party_city` | `<cac:LocatedContractingParty>/<cac:Party>/<cac:PostalAddress>/<cbc:CityName>` |
| `cfs_contracting_party_zip_code` | `<cac:LocatedContractingParty>/<cac:Party>/<cac:PostalAddress>/<cbc:PostalZone>` |
| `cfs_contracting_party_country_code` | `<cac:LocatedContractingParty>/<cac:Party>/<cac:PostalAddress>/<cac:Country>/<cbc:IdentificationCode>` |
| `cfs_result_code` | `<cac:TenderResult>/<cbc:ResultCode>` |
| `cfs_result_description` | `<cac:TenderResult>/<cbc:Description>` |
| `cfs_result_winning_party` | `<cac:TenderResult>/<cac:WinningParty>/<cac:PartyName>/<cbc:Name>` |
| `cfs_result_winning_party_id` | `<cac:TenderResult>/<cac:WinningParty>/<cac:PartyIdentification>/<cbc:ID>` |
| `cfs_result_sme_awarded_indicator` | `<cac:TenderResult>/<cbc:SMEAwardedIndicator>` |
| `cfs_result_award_date` | `<cac:TenderResult>/<cbc:AwardDate>` |
| `cfs_result_tax_exclusive_amount` | `<cac:TenderResult>/<cac:AwardedTenderedProject>/<cac:LegalMonetaryTotal>/<cbc:TaxExclusiveAmount>` value |
| `cfs_result_tax_exclusive_currency` | `currencyID` attribute from the tax-exclusive amount |
| `cfs_result_payable_amount` | `<cac:TenderResult>/<cac:AwardedTenderedProject>/<cac:LegalMonetaryTotal>/<cbc:PayableAmount>` value |
| `cfs_result_payable_currency` | `currencyID` attribute from the payable amount |
| `cfs_terms_funding_program_code` | `<cac:TenderingTerms>/<cbc:FundingProgramCode>` |
| `cfs_terms_award_criteria_type_code` | `<cac:TenderingTerms>/<cac:AwardingTerms>/<cac:AwardingCriteria>/<cbc:AwardingCriteriaTypeCode>` |
| `cfs_process_end_date` | `<cac:TenderingProcess>/<cac:TenderSubmissionDeadlinePeriod>/<cbc:EndDate>` |
| `cfs_process_procedure_code` | `<cac:TenderingProcess>/<cbc:ProcedureCode>` |
| `cfs_process_urgency_code` | `<cac:TenderingProcess>/<cbc:UrgencyCode>` |
| `cfs_raw_xml` | Entire `<cac-place-ext:ContractFolderStatus>` payload |

Multiple values for the same field (e.g., multiple lots) are automatically concatenated with `_`.

### Memory Tuning

The parser writes Parquet files in batches so each period only keeps `batch_size` worth of entries in memory. Configure the following parameters depending on your available resources:

| Parameter | Default | Effect |
|-----------|---------|--------|
| `batch_size` | 150 | Primary control. Lower values reduce peak memory at the cost of more parquet files. |
| `read_concurrency` | 16 | Controls how many XML files are read simultaneously. Lower values reduce I/O pressure. |
| `concat_batches` | false | When enabled, batch files are merged back into `data/parquet/{mc,pt}/{period}.parquet`. Use only if the period fits comfortably in RAM. |

Output structure:
- Default: `data/parquet/{mc,pt}/{period}/batch_*.parquet`
- With `concat_batches`: `data/parquet/{mc,pt}/{period}.parquet`

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
