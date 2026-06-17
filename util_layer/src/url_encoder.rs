use url::form_urlencoded;

/// Encode a string for use in the path of a URL; uses form_urlencoded,
/// then applies some postfilters to fix things up per the RFC.
/// Can optionally handle strings which are meant to encode a path
/// (ie include '/'es which should NOT be escaped).
/// This is based on https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/util/SdkHttpUtils.java#L66
#[allow(dead_code)]
fn url_encode(value: &str, path: bool) -> String {

    // produces application/x-www-form-urlencoded
    let encoded = form_urlencoded::byte_serialize(value.as_bytes()).collect::<String>();

    // The Java code uses a Matcher to replace certain sequences.
    // ENCODED_CHARACTERS_PATTERN matches  "+", "*", "%7E", "%2F".
    
    let mut result = String::with_capacity(encoded.len());
    let mut chars = encoded.chars();
    
    while let Some(c) = chars.next() {
        match c {
            '+' => result.push_str("%20"),
            '*' => result.push_str("%2A"),
            '%' => {
                // Check next two characters for 7E or 2F
                let mut peek = chars.clone();
                match (peek.next(), peek.next()) {
                    (Some('7'), Some('E')) | (Some('7'), Some('e')) => {
                        result.push('~');
                        chars.next(); // consume '7'
                        chars.next(); // consume 'E'
                    }
                    (Some('2'), Some('F')) | (Some('2'), Some('f')) if path => {
                        result.push('/');
                        chars.next(); // consume '2'
                        chars.next(); // consume 'F'
                    }
                    _ => result.push('%'),
                }
            }
            _ => result.push(c),
        }
    }
    result
}

pub fn url_encode_fast(value: &str, path: bool) -> String {
    const HEX_CHARS: &[u8; 16] = b"0123456789ABCDEF";

    //This will waste a bit of memory, since it will likely allocate more than necessary, but we are okay with that.
    let mut result = Vec::with_capacity(value.len() * 3);
    for &b in value.as_bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'-' | b'_' | b'~' => {
                result.push(b);
            }
            b' ' => {
                result.push(b'%');
                result.push(b'2');
                result.push(b'0');
            }
            b'*' => {
                result.push(b'%');
                result.push(b'2');
                result.push(b'A');
            }
            b'/' => {
                if path {
                    result.push(b'/');
                } else {
                    result.push(b'%');
                    result.push(b'2');
                    result.push(b'F');
                }
            }
            _ => {
                result.push(b'%');
                result.push(HEX_CHARS[(b >> 4) as usize]);
                result.push(HEX_CHARS[(b & 0xF) as usize]);
            }
        }
    }

    String::from_utf8(result).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_encode() {
        assert_eq!(url_encode("foo bar", false), "foo%20bar");
        assert_eq!(url_encode("foo*bar", false), "foo%2Abar");
        assert_eq!(url_encode("foo~bar", false), "foo~bar");
        assert_eq!(url_encode("foo/bar", true), "foo/bar");
        assert_eq!(url_encode("foo/bar", false), "foo%2Fbar");
    }

    #[test]
    fn test_url_encode_fast() {
        assert_eq!(url_encode_fast("foo bar", false), "foo%20bar");
        assert_eq!(url_encode_fast("foo*bar", false), "foo%2Abar");
        assert_eq!(url_encode_fast("foo~bar", false), "foo~bar");
        assert_eq!(url_encode_fast("foo/bar", true), "foo/bar");
        assert_eq!(url_encode_fast("foo/bar", false), "foo%2Fbar");
        assert_eq!(url_encode_fast("!@#$", false), "%21%40%23%24");
    }

    #[test]
    fn test_compare_implementations() {
        let cases = [
            ("hello world", true),
            ("hello world", false),
            ("path/to/file", true),
            ("path/to/file", false),
            ("!@#$%^&*()_+", true),
            ("!@#$%^&*()_+", false),
            ("~`[]{};:'\",.<>/?", true),
            ("~`[]{};:'\",.<>/?", false),
            ("测试", true),
            ("测试", false),
            ("", true),
            ("", false),
        ];
        for (s, p) in cases {
            assert_eq!(url_encode(s, p), url_encode_fast(s, p), "Failed for case: ({:?}, {})", s, p);
        }
    }

    #[test]
    #[ignore]
    fn bench_url_encode() {
        let s = "This is a reasonably long string with some / and * and ~ and other characters that might need encoding! @ # $ % ^ & * ( ) _ +";
        let start = std::time::Instant::now();
        for _ in 0..100_000 {
            url_encode(s, true);
        }
        println!("url_encode took: {:?}", start.elapsed());
    }

    #[test]
    #[ignore]
    fn bench_url_encode_fast() {
        let s = "This is a reasonably long string with some / and * and ~ and other characters that might need encoding! @ # $ % ^ & * ( ) _ +";
        let start = std::time::Instant::now();
        for _ in 0..100_000 {
            url_encode_fast(s, true);
        }
        println!("url_encode_fast took: {:?}", start.elapsed());
    }
}
