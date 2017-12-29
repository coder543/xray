use database::Database;
use errors::StrError;

impl Database {
    pub fn stats(&self) -> Result<(), StrError> {
        Err("Stats hasn't been implemented yet!")?
    }
}
