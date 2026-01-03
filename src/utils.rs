use std::time::Duration;

pub fn format_duration(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

pub fn mb_from_bytes(bytes: u64) -> f64 {
    bytes as f64 / 1_048_576.0
}

pub fn round_two_decimals(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn format_duration_zero() {
        assert_eq!(format_duration(Duration::from_secs(0)), "00:00:00");
    }

    #[test]
    fn format_duration_rollover() {
        assert_eq!(format_duration(Duration::from_secs(61)), "00:01:01");
        assert_eq!(format_duration(Duration::from_secs(3661)), "01:01:01");
    }

    #[test]
    fn mb_from_bytes_converts_megabytes() {
        assert_eq!(mb_from_bytes(0), 0.0);
        assert_eq!(mb_from_bytes(1_048_576), 1.0);
    }

    #[test]
    fn round_two_decimals_rounds_half_up() {
        assert_eq!(round_two_decimals(1.234), 1.23);
        assert_eq!(round_two_decimals(1.235), 1.24);
    }
}
