// Data source URLs
pub const MINOR_CONTRACTS: &str = "https://www.hacienda.gob.es/es-es/gobiernoabierto/datos%20abiertos/paginas/contratosmenores.aspx";
pub const PUBLIC_TENDERS: &str = "https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/LicitacionesContratante.aspx";

// CLI Metadata
pub const APP_VERSION: &str = "0.9.0";
pub const APP_AUTHOR: &str = "Alvaro Carranza <alvarocarranzacarrion@gmail.com>";
pub const APP_ABOUT: &str = "Downloads and parses Spanish Public Procurement Data (SPPD)";

// Period help text
pub const PERIOD_HELP_TEXT: &str = "Period (YYYY or YYYYMM format, e.g., 202301)";

// Selectors and Patterns
pub const ZIP_LINK_SELECTOR: &str = r#"a[href$=".zip"]"#;
pub const PERIOD_REGEX_PATTERN: &str = r"_(\d+)\.zip$";

// Procurement type aliases
pub const MINOR_CONTRACTS_ALIASES: &[&str] = &["mc", "minor-contracts", "min"];
pub const PUBLIC_TENDERS_ALIASES: &[&str] = &["pt", "pub", "public-tenders"];
