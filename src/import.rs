use database::Database;

impl Database {
    pub fn import(&mut self, sources: Vec<String>) -> Result<(), String> {
        println!("sources: {:?}", sources);
        Err("Import hasn't been implemented yet!")?
    }
}