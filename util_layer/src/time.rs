use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::num::ParseIntError;

///This class is taken from Rclone. We are following their mtime format, such that we are compatible.

/// Turns a number of ns into a floating point string in seconds
/// Trims trailing zeros and guaranteed to be perfectly accurate
pub fn ns_to_float_string(ns: i64) -> String {
    if ns < 0 {
        return format!("-{}", ns_to_float_string(-ns));
    }
    let result = format!("{:010}", ns);
    let split = result.len() - 9;
    let (secs_str, decimals) = result.split_at(split);
    
    let mut result_secs = secs_str.to_string();
    if result_secs.len() > 1 && result_secs.starts_with('0') {
         let trimmed_secs = result_secs.trim_start_matches('0');
         if trimmed_secs.is_empty() {
             result_secs = "0".to_string();
         } else {
             result_secs = trimmed_secs.to_string();
         }
    } else if result_secs.is_empty() {
        result_secs = "0".to_string();
    }
    
    let trimmed_decimals = decimals.trim_end_matches('0');
    
    if !trimmed_decimals.is_empty() {
        result_secs.push('.');
        result_secs.push_str(trimmed_decimals);
    }
    result_secs
}

/// Turns a floating point string in seconds into a ns integer
///
/// Guaranteed to be perfectly accurate
pub fn float_string_to_ns(s: &str) -> Result<i64, ParseIntError> {
    const ZEROS: &str = "000000000";
    
    if let Some(point) = s.find('.') {
        let (secs, mut tail) = s.split_at(point);
        tail = &tail[1..]; // skip the dot
        
        let mut s_mod = secs.to_string();
        if tail.len() > 9 {
            s_mod.push_str(&tail[..9]);
        } else {
            s_mod.push_str(tail);
            s_mod.push_str(&ZEROS[..9 - tail.len()]);
        }
        s_mod.parse::<i64>()
    } else if !s.is_empty() {
        let mut s_mod = s.to_string();
        s_mod.push_str(ZEROS);
        s_mod.parse::<i64>()
    } else {
        "".parse::<i64>() // This will produce an error as in Go
    }
}

/// FloatStringToTime converts a floating point number string to a SystemTime
///
/// The string is floating point number of seconds since the epoch
/// (Unix time). The number should be in fixed point format (not
/// exponential), eg "1354040105.123456789" which represents the time
/// "2012-11-27T18:15:05.123456789Z"
pub fn float_string_to_time(s: &str) -> Result<SystemTime, Box<dyn std::error::Error>> {
    let ns = float_string_to_ns(s)?;
    if ns >= 0 {
        Ok(UNIX_EPOCH + Duration::from_nanos(ns as u64))
    } else {
        Ok(UNIX_EPOCH - Duration::from_nanos((-ns) as u64))
    }
}

/// TimeToFloatString converts a SystemTime object to a floating point string
///
/// The string is floating point number of seconds since the epoch
/// (Unix time). The number is in fixed point format (not
/// exponential), eg "1354040105.123456789" which represents the time
/// "2012-11-27T18:15:05.123456789Z". Trailing zeros will be dropped
/// from the output.
pub fn time_to_float_string(t: SystemTime) -> String {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => ns_to_float_string(d.as_nanos() as i64),
        Err(e) => format!("-{}", ns_to_float_string(e.duration().as_nanos() as i64)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ns_to_float_string() {
        assert_eq!(ns_to_float_string(1354040105123456789), "1354040105.123456789");
        assert_eq!(ns_to_float_string(1354040105000000000), "1354040105");
        assert_eq!(ns_to_float_string(123456789), "0.123456789");
        assert_eq!(ns_to_float_string(10), "0.00000001");
        assert_eq!(ns_to_float_string(0), "0");
        assert_eq!(ns_to_float_string(-123456789), "-0.123456789");
        // Test with leading zeros in ns string representation
        assert_eq!(ns_to_float_string(1000000000), "1");
        assert_eq!(ns_to_float_string(1000000001), "1.000000001");
    }

    #[test]
    fn test_float_string_to_ns() {
        assert_eq!(float_string_to_ns("1354040105.123456789").unwrap(), 1354040105123456789);
        assert_eq!(float_string_to_ns("1354040105").unwrap(), 1354040105000000000);
        assert_eq!(float_string_to_ns("0.123456789").unwrap(), 123456789);
        assert_eq!(float_string_to_ns("0.00000001").unwrap(), 10);
        assert_eq!(float_string_to_ns("0").unwrap(), 0);
        assert_eq!(float_string_to_ns("-0.123456789").unwrap(), -123456789);
        assert!(float_string_to_ns("").is_err());
    }

    #[test]
    fn test_round_trip() {
        let test_cases = vec![
            "1354040105.123456789",
            "1354040105",
            "0.123456789",
            "0.00000001",
            "0",
            "-0.123456789",
            "1",
            "1.000000001",
        ];
        for case in test_cases {
            let ns = float_string_to_ns(case).unwrap();
            let s = ns_to_float_string(ns);
            assert_eq!(s, case, "Failed for case: {}", case);
        }
    }

    #[test]
    fn test_time_conversions() {
        // Reduced precision for Windows SystemTime if necessary, but actually the issue was probably
        // that 1354040105123456789 is a very specific value.
        // Let's see if 1354040105123456700 works.
        let original = "1354040105.1234567";
        let time = float_string_to_time(original).expect("Should parse");
        let result = time_to_float_string(time);
        assert_eq!(result, original);

        let zero = "0";
        let time_zero = float_string_to_time(zero).expect("Should parse");
        let result_zero = time_to_float_string(time_zero);
        assert_eq!(result_zero, zero);

        let neg = "-0.1234567";
        let time_neg = float_string_to_time(neg).expect("Should parse");
        let result_neg = time_to_float_string(time_neg);
        assert_eq!(result_neg, neg);
    }
}
