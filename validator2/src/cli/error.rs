

/// An error that can occur on CLI activities
#[derive(Debug)]
pub enum CliError {
    FileSystemError(String),

    PythonLoadError(String),

    /// An unknown python subsystem error
    PythonSystemError,
}
