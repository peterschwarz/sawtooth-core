use pyo3::{Python,
           PyResult,
           PyModule,
           GILGuard,
           ToPyPointer,
           };


pub fn init() -> (GILGuard, Python) {
    let gil = Python::acquire_gil();
    let python = gil.python();
    (gil, python)
}

// Python Briging Functions
pub fn load_py_module<'p>(python: Python<'p>, module_name: &str) -> Result<&'p PyModule, CliError> {
    python.import("sawtooth_validator.server.cli")
        .map_err(|err| {
             let traceback: String = if let Some(traceback) = err.ptraceback {
                 match traceback.extract(python) {
                     Ok(s) => s,
                     Err(_) => return CliError::PythonSystemError
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

pub enum PythonBridgeError 
{
    UnableToLoadModule(String)
}
