use database::Database;
use errors::StrError;

impl Database {
    pub fn stats(&self) -> Result<(), StrError> {
        Err("Stats command hasn't been implemented yet!")?
    }
}
