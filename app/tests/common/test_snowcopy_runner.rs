use std::process::Output;
use util_layer::error_codes::ErrorCode;

pub struct SnowcopyOutput {
    output: Output,
    combined_logs : String,
    combined_log_lines: Vec<String>,
    stderr: String,
    stdout: String,
}
impl SnowcopyOutput {
    pub fn new(output: Output) -> Self {
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        let combined_logs = format!("{}\n{}", stdout, stderr);
        let combined_log_lines: Vec<String> = combined_logs.lines().map(|line| line.to_string()).collect();
        Self {
            output,
            combined_logs,
            combined_log_lines,
            stderr: stderr.to_string(),
            stdout: stdout.to_string(),
        }
    }

    pub fn is_success(&self) -> bool {
        self.output.status.success()
    }

    pub fn is_failure(&self, code: ErrorCode) -> bool {
        !self.output.status.success() && code == ErrorCode::HttpUnsafeError
    }

    pub fn get_status(&self) -> &std::process::ExitStatus {
        &self.output.status
    }

    pub fn get_combined_logs(&self) -> &str {
        &self.combined_logs
    }

    pub fn get_combined_log_lines(&self) -> &Vec<String> {
        &self.combined_log_lines
    }

    pub fn get_stderr(&self) -> &str {
        &self.stderr
    }

    pub fn get_stdout(&self) -> &str {
        &self.stdout
    }
    
    pub fn file_is_uploaded_with_multipart(&self, test_file_name: &str) -> bool {
        self.get_combined_logs().contains(format!("{}?uploadId=", test_file_name).as_str())
    }

}