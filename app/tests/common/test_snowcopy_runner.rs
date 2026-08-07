use std::process::Output;

pub struct SnowcopyRunnerOutput {
    output: Output,
    combined_logs : String,
    combined_log_lines: Vec<String>,
}

impl SnowcopyRunnerOutput {
    pub fn new(output: Output) -> Self {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let combined_logs = format!("{}\n{}", stdout, stderr);
        let combined_log_lines: Vec<String> = combined_logs.lines().map(|line| line.to_string()).collect();
        Self {
            output,
            combined_logs,
            combined_log_lines,
        }
    }

    pub fn is_success(&self) -> bool {
        self.output.status.success()
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

}