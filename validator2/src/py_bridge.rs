use pyo3::{Python,
           PyResult,
           PyModule,
           ToPyPointer,
           PyObjectRef,
           PyErr,
           PyDict,
           ObjectProtocol,
           };

// Python Briging Functions
pub fn load_py_module<'p>(python: Python<'p>, module_name: &str) -> Result<&'p PyModule, PythonBridgeError> {
    python.import(module_name).map_err(PythonBridgeError::from)
}

pub fn import_from<'p>(python: Python<'p>, module_name: &str, import_name: &str)
    -> Result<&'p PyObjectRef, PythonBridgeError>
{
    let module = python.import(module_name).map_err(PythonBridgeError::from)?;

    module.get(import_name).map_err(PythonBridgeError::from)
}

#[derive(Debug)]
pub enum PythonBridgeError 
{
    UnknownError,
    PythonError(String),
}

impl From<PyErr> for PythonBridgeError {
    fn from(err: PyErr) -> Self {
        let python = unsafe { Python::assume_gil_acquired() };
        let locals = PyDict::new(python);
        locals.set_item("e", err).unwrap();
        let typename: String = match python.eval("type(e).__name__", None, Some(&locals)) {
            Ok(res) => match res.extract() {
                Ok(name) => name,
                Err(_) => "<Unable to extract Name>".to_string()
            },
            Err(_) => "<Unable to extract Name>".to_string()
        };

        let message: String = match python.eval("str(e)", None, Some(&locals)) {
            Ok(res) => match res.extract() {
                Ok(msg) => msg,
                Err(_) => "<Unable to extract Message>".to_string()
            },
            Err(_) => "<Unable to extract Message>".to_string()
        };

        // TODO: Let's get more information out of the exception
        PythonBridgeError::PythonError(
            format!("{}: {}",
                    typename,
                    message
                    ))
    }
}
