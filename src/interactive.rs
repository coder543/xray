use database::Database;
use errors::StrError;
use rustyline;
use rustyline::error::ReadlineError;

impl Database {
    pub fn interactive(&mut self) -> Result<(), StrError> {
        let mut rl = rustyline::Editor::<()>::new();
        loop {
            let readline = rl.readline(">> ");
            match readline {
                Ok(ref exit) if exit == "exit" => break,
                Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
                Err(err) => return Err(format!("{:?}", err))?,
                Ok(line) => self.search(line.split_whitespace().map(|x| x.into()).collect())?,
            }
        }

        //exit the process for now to avoid the slow Drop process for hundreds of thousands of objects
        ::std::process::exit(0);
    }
}
