use crate::errors::AppResult;
use regex::Regex;
use scraper::{Html, Selector};
use std::collections::BTreeMap;
use std::sync::OnceLock;
use tracing::info;
use url::Url;

// Data source URLs
const MINOR_CONTRACTS_URL: &str = "https://www.hacienda.gob.es/es-es/gobiernoabierto/datos%20abiertos/paginas/contratosmenores.aspx";
const PUBLIC_TENDERS_URL: &str = "https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/LicitacionesContratante.aspx";

// Selectors and Patterns
const ZIP_LINK_SELECTOR: &str = r#"a[href$=".zip"]"#;
const PERIOD_REGEX_PATTERN: &str = r"_(\d+)\.zip$";

/// Cached regex for extracting period identifiers from ZIP filenames.
/// Compiled once at initialization for performance.
static PERIOD_REGEX: OnceLock<Regex> = OnceLock::new();

/// Cached CSS selector for ZIP file links.
/// Compiled once at initialization for performance.
static ZIP_LINK_SELECTOR_CACHED: OnceLock<Selector> = OnceLock::new();

/// Fetches all available ZIP file links from both procurement data sources.
///
/// This function sequentially fetches links from both the minor contracts and
/// public tenders data source pages. It parses HTML to extract ZIP file links
/// and extracts period identifiers (e.g., "202301") from filenames.
///
/// # Returns
///
/// Returns a tuple containing maps of period strings to download URLs:
/// - **First element**: Minor contracts links (period -> URL)
/// - **Second element**: Public tenders links (period -> URL)
///
/// # Errors
///
/// Returns an error if:
/// - Network requests fail
/// - HTML parsing fails
/// - URLs cannot be parsed
///
pub async fn fetch_all_links() -> AppResult<(BTreeMap<String, String>, BTreeMap<String, String>)> {
    let client = reqwest::Client::new();
    // Sequential fetch: simple and reliable for two landing pages.
    info!("Fetching minor contracts links");
    let minor_links = fetch_zip(&client, MINOR_CONTRACTS_URL).await?;
    info!(
        periods_found = minor_links.len(),
        "Minor contracts links fetched"
    );

    info!("Fetching public tenders links");
    let public_links = fetch_zip(&client, PUBLIC_TENDERS_URL).await?;
    info!(
        periods_found = public_links.len(),
        "Public tenders links fetched"
    );

    Ok((minor_links, public_links))
}

/// Fetches ZIP file links from a single procurement data page.
///
/// Downloads the HTML content from the given URL and parses it to extract
/// all ZIP file download links. Period identifiers are extracted from filenames
/// using a regex pattern that matches `_YYYYMM.zip` or similar formats.
///
/// # Arguments
///
/// * `client` - HTTP client to use for the request
/// * `input_url` - URL of the page containing ZIP file links (e.g., the minor contracts
///   or public tenders landing page)
///
/// # Returns
///
/// A map from period strings (e.g., "202301") to absolute download URLs.
///
/// # Errors
///
/// Returns an error if:
/// - The HTTP request fails
/// - The URL cannot be parsed
/// - HTML parsing fails
///
pub async fn fetch_zip(
    client: &reqwest::Client,
    input_url: &str,
) -> AppResult<BTreeMap<String, String>> {
    // parse the base URL
    let base_url = Url::parse(input_url)?;

    // fetch the page content
    let response = client
        .get(base_url.as_str())
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    parse_zip_links(&response, &base_url)
}

/// Parses HTML content and extracts ZIP file links, extracting period identifiers from filenames.
///
/// This function searches for all `<a>` tags with `href` attributes ending in `.zip`,
/// extracts period identifiers from filenames using a regex pattern (e.g., `_202301.zip`),
/// and resolves relative URLs to absolute URLs using the base URL.
///
/// # Returns
///
/// Returns a map where keys are period strings (e.g., "202301") and values are absolute URLs.
///
pub fn parse_zip_links(html: &str, base_url: &Url) -> AppResult<BTreeMap<String, String>> {
    let document = Html::parse_document(html);

    let mut links: BTreeMap<String, String> = BTreeMap::new();

    let selector = ZIP_LINK_SELECTOR_CACHED.get_or_init(|| {
        Selector::parse(ZIP_LINK_SELECTOR).expect("ZIP_LINK_SELECTOR is a valid CSS selector")
    });

    let period_regex = PERIOD_REGEX.get_or_init(|| {
        Regex::new(PERIOD_REGEX_PATTERN).expect("PERIOD_REGEX_PATTERN is a valid regex pattern")
    });

    for url in document
        .select(selector)
        .filter_map(|el| el.value().attr("href"))
        .filter_map(|href| base_url.join(href).ok())
    {
        if let Some(filename) = url.path_segments().and_then(|mut s| s.next_back()) {
            if let Some(m) = period_regex.captures(filename).and_then(|c| c.get(1)) {
                links.insert(m.as_str().to_string(), url.to_string());
            }
        }
    }

    Ok(links)
}

#[cfg(test)]
mod tests {
    use super::parse_zip_links;
    use url::Url;

    #[test]
    fn test_parse_zip_links_basic() {
        let html = r#"
            <html>
            <body>
              <a href="files/data_202301.zip">202301</a>
              <a href="/downloads/data_202302.zip">202302</a>
              <a href="https://other.example.com/attachments/data_202303.zip">202303</a>
              <a href="not_a_zip.txt">skip</a>
            </body>
            </html>
        "#;

        let base = Url::parse("https://example.com/path/").expect("base url");
        let result = parse_zip_links(html, &base).expect("parse succeeds");

        // Should contain the three detected periods with absolute URLs
        assert_eq!(
            result.get("202301").unwrap(),
            "https://example.com/path/files/data_202301.zip"
        );
        assert_eq!(
            result.get("202302").unwrap(),
            "https://example.com/downloads/data_202302.zip"
        );
        assert_eq!(
            result.get("202303").unwrap(),
            "https://other.example.com/attachments/data_202303.zip"
        );
    }

    #[test]
    fn test_parse_zip_links_no_capture() {
        let html = r#"
            <html><body>
              <a href="files/data202301.zip">no underscore</a>
              <a href="files/data_abc.zip">non-numeric</a>
            </body></html>
        "#;

        let base = Url::parse("https://example.com/").expect("base url");
        let result = parse_zip_links(html, &base).expect("parse succeeds");
        // No valid numeric captures -> empty
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_zip_links_multiple_underscores_uses_last_capture() {
        let html = r#"
            <html><body>
              <a href="files/prefix_2023_202301.zip">multi</a>
            </body></html>
        "#;

        let base = Url::parse("https://example.com/").expect("base url");
        let result = parse_zip_links(html, &base).expect("parse succeeds");
        // Expect to capture the last numeric group (202301)
        assert_eq!(
            result.get("202301").unwrap(),
            "https://example.com/files/prefix_2023_202301.zip"
        );
    }

    #[test]
    fn test_parse_zip_links_duplicate_periods_last_wins() {
        let html = r#"
            <html><body>
              <a href="files/data_202301.zip">first</a>
              <a href="files/other_202301.zip">second</a>
            </body></html>
        "#;

        let base = Url::parse("https://example.com/").expect("base url");
        let result = parse_zip_links(html, &base).expect("parse succeeds");
        // BTreeMap insert will keep the last inserted value for the same key
        assert_eq!(
            result.get("202301").unwrap(),
            "https://example.com/files/other_202301.zip"
        );
    }

    #[test]
    fn test_parse_zip_links_relative_paths_resolve() {
        let html = r#"
            <html><body>
              <a href="./files/data_202304.zip">rel</a>
              <a href="../up/data_202305.zip">up</a>
            </body></html>
        "#;

        let base = Url::parse("https://example.com/path/sub/").expect("base url");
        let result = parse_zip_links(html, &base).expect("parse succeeds");
        assert_eq!(
            result.get("202304").unwrap(),
            "https://example.com/path/sub/files/data_202304.zip"
        );
        assert_eq!(
            result.get("202305").unwrap(),
            "https://example.com/path/up/data_202305.zip"
        );
    }
}
