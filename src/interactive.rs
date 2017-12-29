use rustyline;
use rustyline::error::ReadlineError;
use database::Database;
use errors::StrError;

impl Database {
    pub fn interactive(&mut self) -> Result<(), StrError> {
        let mut rl = rustyline::Editor::<()>::new();
        loop {
            let readline = rl.readline(">> ");
            match readline {
                Ok(ref exit) if exit == "exit" => break,
                Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
                Err(err) => return Err(format!("{:?}", err))?,
                Ok(line) => self.search(vec![line])?,
            }
        }
        Ok(())
    }
}