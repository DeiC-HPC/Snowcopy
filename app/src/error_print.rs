use util_layer::error_codes::UserFacingError;

pub fn print_fatal_error(error: &UserFacingError) {
    print_internal(error, ConsoleColor::Red, "FATAL ERROR");
}

pub fn print_error(error: &UserFacingError) {
    print_internal(error, ConsoleColor::Yellow, "ERROR");
}

fn print_internal(error: &UserFacingError, color: ConsoleColor, headline: &str) {
    let code = error.code();
    let message = error.message();
    let ansi = color.ansi_code();

    eprintln!("\n\x1b[1;{}m{}\x1b[0m", ansi, "=".repeat(80));
    eprintln!(
        "\x1b[1;{}m{} (Code: {})\x1b[0m",
        ansi,
        headline,
        code.as_i32()
    );
    eprintln!("\x1b[1;{}m{}\x1b[0m", ansi, "-".repeat(80));
    eprintln!("{}", message);
    eprintln!("\x1b[1;{}m{}\x1b[0m\n", ansi, "=".repeat(80));
}


enum ConsoleColor {
    #[allow(dead_code)]
    Red,
    #[allow(dead_code)]
    Yellow,
    #[allow(dead_code)]
    Green,
    #[allow(dead_code)]
    Blue,
    #[allow(dead_code)]
    Magenta,
    #[allow(dead_code)]
    Cyan,
    #[allow(dead_code)]
    White,
}

impl ConsoleColor {
    fn ansi_code(self) -> &'static str {
        match self {
            ConsoleColor::Red => "31",
            ConsoleColor::Yellow => "33",
            ConsoleColor::Green => "32",
            ConsoleColor::Blue => "34",
            ConsoleColor::Magenta => "35",
            ConsoleColor::Cyan => "36",
            ConsoleColor::White => "37",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use util_layer::error_codes::ErrorCode;

    #[test]
    fn test_print_fatal_error_logic() {
        let err = UserFacingError::new(ErrorCode::DiskIoError, "Test error message".to_string());
        // Since it prints to stderr, we just call it to ensure no panics and it works as intended.
        print_fatal_error(&err);
    }

    #[test]
    fn test_print_error_logic() {
        let err = UserFacingError::new(ErrorCode::SkippedInvalidKeysDownload, "Test error message".to_string());
        // Since it prints to stderr, we just call it to ensure no panics and it works as intended.
        print_error(&err);
    }
}
