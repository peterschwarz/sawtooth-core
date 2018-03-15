use pyo3::{Python,
           PyResult,
           PyModule,
           ToPyPointer,
           };



// Python Briging Functions
pub fn load_py_module<'p>(python: Python<'p>, module_name: &str) -> Result<&'p PyModule, PythonBridgeError> {
    python.import("sawtooth_validator.server.cli")
        .map_err(|err| {
             let traceback: String = if let Some(traceback) = err.ptraceback {
                 match traceback.extract(python) {
                     Ok(s) => s,
                     Err(_) => return PythonBridgeError::UnknownError
                 }
             } else {
                 String::from("<No Traceback>")
             };

             PythonBridgeError::UnableToLoadModule(
                 format!("Unable to load {}\n{}",
                         module_name,
                         traceback))
        })
}

#[derive(Debug)]
pub enum PythonBridgeError 
{
    UnknownError,
    UnableToLoadModule(String)
}
