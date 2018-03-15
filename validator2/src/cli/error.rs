use conf::LocalConfigurationError;
use py_bridge::PythonBridgeError;

/// An error that can occur on CLI activities
#[derive(Debug)]
pub enum CliError {
    FileSystemError(String),

    ArgumentError(String),
    ConfigurationError(LocalConfigurationError),

    PythonLoadError(PythonBridgeError),

    /// An unknown python subsystem error
    PythonSystemError,
}

impl From<LocalConfigurationError> for CliError {
    fn from(error: LocalConfigurationError) -> Self {
        CliError::ConfigurationError(error)
    }
}

impl From<PythonBridgeError> for CliError {
    fn from(error: PythonBridgeError) -> Self {
        CliError::PythonLoadError(error)
    }
}
