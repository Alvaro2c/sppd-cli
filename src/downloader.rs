use crate::config::*;
use reqwest;
use scraper::{Html, Selector};
use std::collections::HashMap;
use std::str::FromStr;
use url::Url;

pub fn fetch_all_links() -> Result<(HashMap<String, String>, HashMap<String, String>), Box<dyn std::error::Error>> {
    let minor_links = fetch_zip(MINOR_CONTRACTS)?;
    let public_links = fetch_zip(PUBLIC_TENDERS)?;
    Ok((minor_links, public_links))
}


pub fn fetch_zip(input_url: &str) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    // parse the base URL
    let base_url = Url::parse(input_url)?;

    // fetch the page content
    let response = reqwest::blocking::get(base_url.as_str())?
        .error_for_status()?
        .text()?;
    let document = Html::parse_document(&response);

    // selector to find all links ending with .zip
    let selector = Selector::parse(r#"a[href$=".zip"]"#).unwrap();

    let mut links: HashMap<String, String> = HashMap::new();
    let re = regex::Regex::new(r"_(\d+)\.zip$")?;

    for url in document
        .select(&selector)
        .filter_map(|el| el.value().attr("href"))
        .filter_map(|href| base_url.join(href).ok())
    {
        if let Some(filename) = url.path_segments().and_then(|s| s.last()) {
            if let Some(m) = re.captures(filename).and_then(|c| c.get(1)) {
                links.insert(m.as_str().to_string(), url.to_string());
            }
        }
    }

    Ok(links)
}

pub fn filter_periods_by_range(
    links: &HashMap<String, String>,
    start_period: Option<&str>,
    end_period: Option<&str>,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let mut filtered = HashMap::new();

    let start_period_num = start_period.and_then(|s| u32::from_str(s).ok());
    let end_period_num = end_period.and_then(|s| u32::from_str(s).ok());

    // Get sorted list of available periods
    let mut available_periods: Vec<_> = links.keys().collect();
    available_periods.sort();
    let available_str = available_periods
        .iter()
        .map(|p| p.as_str())
        .collect::<Vec<_>>()
        .join(", ");

    // Validate that specified periods exist in links
    if let Some(start) = start_period {
        if !links.contains_key(start) {
            return Err(format!(
                "Period '{}' is not available. Available periods: {}",
                start, available_str
            )
            .into());
        }
    }

    if let Some(end) = end_period {
        if !links.contains_key(end) {
            return Err(format!(
                "Period '{}' is not available. Available periods: {}",
                end, available_str
            )
            .into());
        }
    }

    for (period, url) in links {
        if let Ok(period_num) = u32::from_str(period) {
            let in_range = match (start_period_num, end_period_num) {
                (Some(start), Some(end)) => period_num >= start && period_num <= end,
                (Some(start), None) => period_num >= start,
                (None, Some(end)) => period_num <= end,
                (None, None) => true,
            };

            if in_range {
                filtered.insert(period.clone(), url.clone());
            }
        }
    }

    Ok(filtered)
}
