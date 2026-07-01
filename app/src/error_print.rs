use util_layer::error_codes::FatalError;

pub fn print_fatal_error(error: &FatalError) {
    let code = error.code();
    let message = error.message();

    eprintln!("\n\x1b[1;31m{}\x1b[0m", "=".repeat(80));
    eprintln!("\x1b[1;31mFATAL ERROR (Code: {})\x1b[0m", code.as_i32());
    eprintln!("\x1b[1;31m{}\x1b[0m", "-".repeat(80));
    eprintln!("{}", message);
    eprintln!("\x1b[1;31m{}\x1b[0m\n", "=".repeat(80));
}

#[cfg(test)]
mod tests {
    use super::*;
    use util_layer::error_codes::FatalErrorCode;

    #[test]
    fn test_print_fatal_error_logic() {
        let err = FatalError::new(FatalErrorCode::DiskIoError, "Test error message".to_string());
        // Since it prints to stderr, we just call it to ensure no panics and it works as intended.
        // In a real test we might use something like 'gag' to capture stderr, but here we just verify it runs.
        print_fatal_error(&err);
    }
}
