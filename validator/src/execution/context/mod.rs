mod errors;

use database::lmdb::LmdbDatabase;
use state::merkle::MerkleDatabase;

use execution::context::errors::ContextError;

struct ContextManager {
    database: LmdbDatabase,
}

impl ContextManager {
    pub fn new(database: LmdbDatabase) -> Self {
        ContextManager { database }
    }

    pub fn get_first_root(&self) -> Result<String, ContextError> {
        Ok(MerkleDatabase::new(self.database.clone(), None)?.get_merkle_root())
    }

    pub fn create_context(
        &self,
        state_hash: &str,
        base_contexts: &[&str],
        inputs: &[&str],
        outputs: &[&str],
    ) -> Result<String, ContextError> {
        Err(ContextError::CreateFailure(format!(
            "Unable to create context with base {:?}",
            base_contexts
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs::remove_file;
    use std::panic;
    use std::path::Path;
    use std::str::from_utf8;
    use std::thread;

    use hashlib::sha512_digest_bytes;
    use rand::prelude::*;
    use rand::thread_rng;
    use protobuf;

    use super::*;
    use database::error::DatabaseError;
    use database::lmdb::LmdbContext;
    use database::lmdb::LmdbDatabase;
    use state::{merkle, StateReader};
    use proto::events;

    /// Tests that get_execution_results returns the correct value.
    #[test]
    fn test_execution_results() {
        run_test(|db_of_record_path, _db_of_results_path| {
            let db_of_record = make_lmdb(db_of_record_path);

            let addr1 = create_address();
            let addr2 = create_address();

            let context_manager = ContextManager::new(db_of_record);

            let context_id = context_manager.create_context(
                &context_manager.get_first_root().unwrap(),
                &[],
                &[&addr1, &addr2],
                &[&addr1, &addr2],
            ).unwrap();

            let sets = vec![(addr1.clone(), vec![1u8])];
            let events = vec![make_event("test1"), make_event("test2")];
            let deletes = vec![addr2];
        });
    }

    fn run_test<T>(test: T) -> ()
    where
        T: FnOnce(&str, &str) -> () + panic::UnwindSafe,
    {
        let db_of_record_path = temp_db_path("db_of_record");
        let db_of_results_path = temp_db_path("db_results");

        let test_db_of_record_path = db_of_record_path.clone();
        let test_db_of_results_path = db_of_results_path.clone();
        let result =
            panic::catch_unwind(move || test(&test_db_of_record_path, &test_db_of_results_path));

        remove_file(db_of_record_path).unwrap();
        remove_file(db_of_results_path).unwrap();

        assert!(result.is_ok())
    }

    fn make_lmdb(merkle_path: &str) -> LmdbDatabase {
        let ctx = LmdbContext::new(
            Path::new(merkle_path),
            merkle::INDEXES.len(),
            Some(120 * 1024 * 1024),
        ).map_err(|err| DatabaseError::InitError(format!("{}", err)))
        .unwrap();
        LmdbDatabase::new(ctx, &merkle::INDEXES)
            .map_err(|err| DatabaseError::InitError(format!("{}", err)))
            .unwrap()
    }

    fn make_event(event_name: &str) -> events::Event {
        let mut event = events::Event::new();
        event.set_event_type(event_name.to_string());
        event.set_data(event_name.as_bytes().to_vec());

        let mut attr = events::Event_Attribute::new();
        attr.set_key(event_name.to_string());
        attr.set_value(event_name.to_string());
        event.set_attributes(protobuf::RepeatedField::from_vec(vec![attr]));

        event
    }

    fn temp_db_path(file_root: &str) -> String {
        let mut temp_dir = env::temp_dir();

        let thread_id = thread::current().id();
        temp_dir.push(format!("{}-{:?}.lmdb", file_root, thread_id));
        println!("temp_file {:?}", &temp_dir);
        temp_dir.to_str().unwrap().to_string()
    }

    fn create_address() -> String {
        let mut rng = thread_rng();
        let mut bytes: Vec<u8> = (0..255).collect();
        bytes.shuffle(&mut rng);
        bytes.truncate(35);
        ::hex::encode(bytes)
    }

    fn hex_hash(b: &[u8]) -> String {
        ::hex::encode(hash(b))
    }

    /// Creates a hash of the given bytes
    fn hash(input: &[u8]) -> Vec<u8> {
        let bytes = sha512_digest_bytes(input);
        let (hash, _rest) = bytes.split_at(bytes.len() / 2);
        hash.to_vec()
    }
}
