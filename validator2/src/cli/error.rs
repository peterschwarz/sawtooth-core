use conf::LocalConfigurationError;

/// An error that can occur on CLI activities
#[derive(Debug)]
pub enum CliError {
    FileSystemError(String),

    ArgumentError(String),
    ConfigurationError(LocalConfigurationError),

    PythonLoadError(String),

    /// An unknown python subsystem error
    PythonSystemError,
}

impl From<LocalConfigurationError> for CliError {
    fn from(error: LocalConfigurationError) -> Self {
        CliError::ConfigurationError(error)
    }
}
