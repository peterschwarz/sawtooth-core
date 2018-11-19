use state::error::StateDatabaseError;

#[derive(Debug)]
pub enum ContextError {
    CreateFailure(String),
    StateDatabaseError(StateDatabaseError),
}

impl From<StateDatabaseError> for ContextError {
    fn from(err: StateDatabaseError) -> Self {
        ContextError::StateDatabaseError(err)
    }
}
