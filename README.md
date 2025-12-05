# SPPD CLI

A command-line tool for fetching and managing data from public tenders and minor contracts in Spain.

## Build

```bash
cargo build --release
```

## Run

```bash
cargo run
```

## Requirements

- Rust 1.56 or later

## Features

- Fetches minor contracts links
- Fetches public tenders links
- Interactive CLI interface

## CLI Usage

The main command is `download`, which fetches ZIP archives for a range of periods.

Basic form:

```bash
cargo run -- download [OPTIONS]
```

Options:

- `-t, --type <TYPE>`: Procurement type to download. Accepted values:
  - `public-tenders` (aliases: `pt`, `pub`) — default
  - `minor-contracts` (aliases: `mc`, `min`)
- `-s, --start <PERIOD>`: Start period (inclusive). Uses the period format described below.
- `-e, --end <PERIOD>`: End period (inclusive).

Period format:

- Periods accept either `YYYY` or `YYYYMM` formats. Examples: `2023`, `202301`.
- Available periods are:
  - Previous years: full years only (`YYYY`)
  - Current year: all months up to the download date (`YYYYMM`)

Download destination:

- Files are saved under the `tmp` directory, in a subdirectory depending on the procurement type:
  - Minor Contracts -> `tmp/mc`
  - Public Tenders -> `tmp/pt`

Examples:

- Download all available public tenders:

```bash
cargo run -- download
```

- Download public tenders for 2023 (year):

```bash
cargo run -- download -t public-tenders -s 2023 -e 2023
```

- Download minor contracts for January 2025:

```bash
cargo run -- download -t mc -s 202501 -e 202501
```

Notes:

- The `--type` value is case-insensitive and supports the listed aliases.
- The CLI will validate that requested periods exist; if a period is not available an error will list available ones.
