/*
 * Copyright 2018 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ------------------------------------------------------------------------------
 */

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::Cursor;

use cbor;
use cbor::encoder::{EncodeError, GenericEncoder};
use cbor::decoder::{DecodeError, GenericDecoder};
use cbor::value::Value;
use cbor::value::Key;
use cbor::value::Bytes;
use cbor::value::Text;

use crypto::digest::Digest;
use crypto::sha2::Sha512;

use database::database::DatabaseError;
use database::lmdb::LmdbDatabase;

const TOKEN_SIZE: usize = 2;

/// Merkle Database
pub struct MerkleDatabase {
    root_hash: String,
    db: LmdbDatabase,
    root_node: Node,
}

#[derive(Debug)]
pub enum MerkleDatabaseError {
    NotFound(String),
    DeserializationError(DecodeError),
    SerializationError(EncodeError),
    InvalidRecord,
    DatabaseError(DatabaseError),
    UnknownError,
}

impl From<DatabaseError> for MerkleDatabaseError {
    fn from(err: DatabaseError) -> Self {
        MerkleDatabaseError::DatabaseError(err)
    }
}

impl From<EncodeError> for MerkleDatabaseError {
    fn from(err: EncodeError) -> Self {
        MerkleDatabaseError::SerializationError(err)
    }
}

impl From<DecodeError> for MerkleDatabaseError {
    fn from(err: DecodeError) -> Self {
        MerkleDatabaseError::DeserializationError(err)
    }
}

impl MerkleDatabase {
    /// Constructs a new MerkleDatabase, backed by a given Database
    ///
    /// An optional starting merkle root may be provided.
    pub fn new(db: LmdbDatabase, merkle_root: Option<&str>) -> Result<Self, MerkleDatabaseError> {
        let root_hash = merkle_root.map_or_else(|| initialize_db(&db), |s| Ok(s.into()))?;
        let root_node = get_node_by_hash(&db, &root_hash)?;

        Ok(MerkleDatabase {
            root_hash,
            root_node,
            db,
        })
    }

    /// Returns the current merkle root for this MerkleDatabase
    pub fn get_merkle_root(&self) -> String {
        self.root_hash.clone()
    }

    /// Sets the current merkle root for this MerkleDatabase
    pub fn set_merkle_root<S: Into<String>>(
        &mut self,
        merkle_root: S,
    ) -> Result<(), MerkleDatabaseError> {
        let new_root = merkle_root.into();
        self.root_node = get_node_by_hash(&self.db, &new_root)?;
        self.root_hash = new_root;
        Ok(())
    }

    /// Returns true if the given address exists in the MerkleDatabase;
    /// false, otherwise.
    pub fn contains(&self, address: &str) -> Result<bool, MerkleDatabaseError> {
        match self.get_by_address(address) {
            Ok(_) => Ok(true),
            Err(MerkleDatabaseError::NotFound(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Returns the data for a given address, if they exist at that node.  If
    /// not, returns None.  Will return an MerkleDatabaseError::NotFound, if the
    /// given address is not in the tree
    pub fn get(&self, address: &str) -> Result<Option<Vec<u8>>, MerkleDatabaseError> {
        Ok(self.get_by_address(address)?.value)
    }

    /// Sets the given data at the given address.
    ///
    /// Returns a Result with the new merkle root hash, or an error if the
    /// address is not in the tree.
    ///
    /// Note, continued calls to get, without changing the merkle root to the
    /// result of this function, will not retrieve the results provided here.
    pub fn set(&self, address: &str, data: &[u8]) -> Result<String, MerkleDatabaseError> {
        let tokens = tokenize_address(address);

        let path_addresses = path_addresses_from_tokens(&tokens, false);

        let mut path_map = self.get_path_by_tokens(&tokens)?;

        let mut child = path_map
            .remove(&path_addresses[0])
            .expect("Path map not correctly generated");
        child.value = Some(data.to_vec());

        let mut batch = Vec::with_capacity(path_addresses.len());
        // initializing this to empty, to make the compiler happy
        let mut key_hash: String = String::new();
        for path_address in path_addresses {
            let (child_key, child_packed) = encode_and_hash(child)?;
            key_hash = child_key;

            let (parent_address, path_branch) = parent_and_branch(&path_address);
            let mut parent = path_map
                .remove(parent_address)
                .expect("Path map not correctly generated");

            parent
                .children
                .insert(path_branch.to_string(), key_hash.clone());

            batch.push((key_hash.clone(), child_packed));

            child = parent;
        }

        let mut new_root = self.root_node.clone();
        new_root
            .children
            .insert(tokens[0].to_string(), key_hash.clone());

        let (root_hash, packed) = encode_and_hash(new_root)?;

        batch.push((root_hash.clone(), packed));

        self.put_batch(batch)?;

        Ok(root_hash)
    }

    /// Deletes the value at the given address.
    ///
    /// Returns a Result with the new merkle root hash, or an error if the
    /// address is not in the tree.
    ///
    /// Note, continued calls to get, without changing the merkle root to the
    /// result of this function, will still retrieve the data at the address
    /// provided
    pub fn delete(&self, address: &str) -> Result<String, MerkleDatabaseError> {
        let tokens = tokenize_address(address);
        let mut path_map = self.get_path_by_tokens(&tokens)?;
        let mut leaf_branch = true;

        let paths = path_addresses_from_tokens(&tokens, true);

        // initializing this to empty, to make the compiler happy
        let mut key_hash: String = String::new();
        let mut batch = Vec::with_capacity(paths.len());
        for path in paths {
            let (parent_address, path_branch) = parent_and_branch(&path);

            let node = path_map
                .remove(&path)
                .expect("Path map not correctly generated");

            if path == "" || !node.children.is_empty() {
                leaf_branch = false;
            }

            if !leaf_branch {
                let (hash_key, packed) = encode_and_hash(node)?;
                key_hash = hash_key.clone();

                if path != "" {
                    let mut parent = path_map
                        .get_mut(parent_address)
                        .expect("Path map not correctly generated");
                    parent
                        .children
                        .insert(path_branch.to_string(), hash_key.clone());
                }

                batch.push((hash_key, packed));
            } else if path != "" {
                let mut parent = path_map
                    .get_mut(parent_address)
                    .expect("Path map not correctly generated");
                if parent.children.remove(path_branch).is_none() {
                    return Err(MerkleDatabaseError::NotFound(format!(
                        "{} is not a key of an existing record",
                        address
                    )));
                }
            }
        }

        self.put_batch(batch)?;
        Ok(key_hash)
    }

    /// Updates the tree with multiple changes.  Applies both set and deletes,
    /// as given.
    ///
    /// If the flag `is_virtual` is set, the values are not written to the
    /// underlying database.
    ///
    /// Returns a Result with the new root hash.
    pub fn update(
        &self,
        set_items: &HashMap<String, Vec<u8>>,
        delete_items: &[String],
        is_virtual: bool,
    ) -> Result<String, MerkleDatabaseError> {
        let mut path_map = HashMap::new();

        for (set_address, set_value) in set_items {
            let tokens = tokenize_address(set_address);
            let mut set_path_map = self.get_path_by_tokens(&tokens)?;

            {
                let node = set_path_map
                    .get_mut(set_address)
                    .expect("Path map not correctly generated");
                node.value = Some(set_value.to_vec());
            }
            path_map.extend(set_path_map);
        }

        for del_address in delete_items.iter() {
            let tokens = tokenize_address(del_address);
            let del_path_map = self.get_path_by_tokens(&tokens)?;
            path_map.extend(del_path_map);
        }

        for del_address in delete_items.iter() {
            path_map.remove(del_address);
            let (mut parent_address, mut path_branch) = parent_and_branch(del_address);
            while parent_address != "" {
                let remove_parent = {
                    let parent_node = path_map
                        .get_mut(parent_address)
                        .expect("Path map not correctly generated");
                    parent_node.children.remove(path_branch);

                    parent_node.children.is_empty()
                };

                if remove_parent {
                    // empty node delete it.
                    path_map.remove(parent_address);
                } else {
                    // found a node that is not empty no need to continue
                    break;
                }

                let (next_parent, next_branch) = parent_and_branch(parent_address);
                parent_address = next_parent;
                path_branch = next_branch;

                if parent_address == "" {
                    let parent_node = path_map
                        .get_mut(parent_address)
                        .expect("Path map not correctly generated");
                    parent_node.children.remove(path_branch);
                }
            }
        }

        let mut sorted_paths: Vec<_> = path_map.keys().cloned().collect();
        // Sort by longest to shortest
        sorted_paths.sort_by(|a, b| b.len().cmp(&a.len()));

        // initializing this to empty, to make the compiler happy
        let mut key_hash: String = String::new();
        let mut batch = Vec::with_capacity(sorted_paths.len());
        for path in sorted_paths {
            let node = path_map
                .remove(&path)
                .expect("Path map keys are out of sink");
            let (hash_key, packed) = encode_and_hash(node)?;
            key_hash = hash_key.clone();

            if path != "" {
                let (parent_address, path_branch) = parent_and_branch(&path);
                let mut parent = path_map
                    .get_mut(parent_address)
                    .expect("Path map not correctly generated");
                parent
                    .children
                    .insert(path_branch.to_string(), hash_key.clone());
            }

            batch.push((hash_key, packed));
        }

        if !is_virtual {
            self.put_batch(batch)?;
        }

        Ok(key_hash)
    }

    pub fn leaves<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> Result<MerkleLeafIterator, MerkleDatabaseError> {
        MerkleLeafIterator::new(self, prefix)
    }

    /// Puts all the items into the database.
    fn put_batch(&self, batch: Vec<(String, Vec<u8>)>) -> Result<(), MerkleDatabaseError> {
        let mut db_writer = self.db.writer()?;
        for (key, value) in batch {
            db_writer.put(key.as_bytes(), &value)?;
        }
        db_writer.commit()?;
        Ok(())
    }

    fn get_by_address(&self, address: &str) -> Result<Node, MerkleDatabaseError> {
        let tokens = tokenize_address(address);

        // There's probably a better way to do this than a clone
        let mut node = self.root_node.clone();

        for token in tokens.iter() {
            node = match node.children.get(&token.to_string()) {
                None => {
                    return Err(MerkleDatabaseError::NotFound(format!(
                        "invalid address {} from root {}",
                        address, self.root_hash
                    )))
                }
                Some(child_hash) => get_node_by_hash(&self.db, child_hash)?,
            }
        }
        Ok(node)
    }

    fn get_path_by_tokens(
        &self,
        tokens: &[&str],
    ) -> Result<HashMap<String, Node>, MerkleDatabaseError> {
        let mut nodes = HashMap::new();

        let mut path = String::new();
        nodes.insert(path.clone(), self.root_node.clone());

        let mut new_branch = false;

        for token in tokens {
            let node = {
                // this is safe to unwrap, because we've just inserted the path in the previous loop
                let child_address = &nodes[&path].children.get(&token.to_string());
                if !new_branch && child_address.is_some() {
                    get_node_by_hash(&self.db, child_address.unwrap())?
                } else {
                    new_branch = true;
                    Node::default()
                }
            };

            path.push_str(token);
            nodes.insert(path.clone(), node);
        }
        Ok(nodes)
    }
}

pub struct MerkleLeafIterator<'a> {
    merkle_db: &'a MerkleDatabase,
    visited: VecDeque<(String, Node)>,
}

impl<'a> MerkleLeafIterator<'a> {
    fn new(
        merkle_db: &'a MerkleDatabase,
        prefix: Option<&'a str>,
    ) -> Result<Self, MerkleDatabaseError> {
        let path = prefix.unwrap_or("");

        let mut visited = VecDeque::new();
        let initial_node = merkle_db.get_by_address(path)?;
        visited.push_front((path.to_string(), initial_node));

        Ok(MerkleLeafIterator { merkle_db, visited })
    }
}

impl<'a> Iterator for MerkleLeafIterator<'a> {
    type Item = Result<(String, Vec<u8>), MerkleDatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.visited.is_empty() {
            return None;
        }

        loop {
            if let Some((path, node)) = self.visited.pop_front() {
                if node.value.is_some() {
                    return Some(Ok((path, node.value.unwrap())));
                }

                // Reverse the list, such that we have an in-order traversal of the
                // children, based on the natural path order.
                for (child_path, hash_key) in node.children.iter().rev() {
                    let child = match get_node_by_hash(&self.merkle_db.db, hash_key) {
                        Ok(node) => node,
                        Err(err) => return Some(Err(err)),
                    };
                    let mut child_address = path.clone();
                    child_address.push_str(child_path);
                    self.visited.push_front((child_address, child));
                }
            } else {
                return None;
            }
        }
    }
}

fn initialize_db(db: &LmdbDatabase) -> Result<String, MerkleDatabaseError> {
    let (hash, packed) = encode_and_hash(Node::default())?;

    let mut db_writer = db.writer()?;
    db_writer.put(hash.as_bytes(), &packed)?;
    db_writer.commit()?;

    Ok(hash)
}

fn encode_and_hash(node: Node) -> Result<(String, Vec<u8>), MerkleDatabaseError> {
    let packed = node.into_bytes()?;
    let hash = hash(&packed);
    Ok((hash, packed))
}

fn parent_and_branch(path: &str) -> (&str, &str) {
    let parent_address = if !path.is_empty() {
        &path[..path.len() - TOKEN_SIZE]
    } else {
        ""
    };

    let path_branch = if !path.is_empty() {
        &path[(path.len() - TOKEN_SIZE)..]
    } else {
        ""
    };

    (parent_address, path_branch)
}

fn tokenize_address(address: &str) -> Box<[&str]> {
    let mut tokens: Vec<&str> = Vec::with_capacity(address.len() / 2);
    let mut i = 0;
    while i < address.len() {
        tokens.push(&address[i..i + 2]);
        i += 2;
    }
    tokens.into_boxed_slice()
}

fn path_addresses_from_tokens(tokens: &[&str], include_root: bool) -> Vec<String> {
    let mut path_addresses = Vec::new();
    let mut i = tokens.len();
    while i > 0 {
        path_addresses.push(
            tokens
                .iter()
                .take(i)
                .map(|s| String::from(*s))
                .collect::<Vec<_>>()
                .join(""),
        );
        i -= 1;
    }

    if include_root {
        path_addresses.push(String::new())
    }

    path_addresses
}

/// Fetch a node by its hash
fn get_node_by_hash(db: &LmdbDatabase, hash: &str) -> Result<Node, MerkleDatabaseError> {
    match db.reader()?.get(hash.as_bytes()) {
        Some(bytes) => Node::from_bytes(&bytes),
        None => Err(MerkleDatabaseError::NotFound(hash.to_string())),
    }
}

/// Internal Node structure of the Radix tree
#[derive(Default, Debug, PartialEq, Clone)]
struct Node {
    value: Option<Vec<u8>>,
    children: BTreeMap<String, String>,
}

impl Node {
    /// Consumes this node and serializes it to bytes
    fn into_bytes(self) -> Result<Vec<u8>, MerkleDatabaseError> {
        let mut e = GenericEncoder::new(Cursor::new(Vec::new()));

        let mut map = BTreeMap::new();
        map.insert(
            Key::Text(Text::Text("v".to_string())),
            match self.value {
                Some(bytes) => Value::Bytes(Bytes::Bytes(bytes)),
                None => Value::Null,
            },
        );

        let children = self.children
            .into_iter()
            .map(|(k, v)| {
                (
                    Key::Text(Text::Text(k.to_string())),
                    Value::Text(Text::Text(v.to_string())),
                )
            })
            .collect();

        map.insert(Key::Text(Text::Text("c".to_string())), Value::Map(children));

        e.value(&Value::Map(map))?;

        Ok(e.into_inner().into_writer().into_inner())
    }

    /// Deserializes the given bytes to a Node
    fn from_bytes(bytes: &[u8]) -> Result<Node, MerkleDatabaseError> {
        let input = Cursor::new(bytes);
        let mut decoder = GenericDecoder::new(cbor::Config::default(), input);
        let decoder_value = decoder.value()?;
        let (val, children_raw) = match decoder_value {
            Value::Map(mut root_map) => (
                root_map.remove(&Key::Text(Text::Text("v".to_string()))),
                root_map.remove(&Key::Text(Text::Text("c".to_string()))),
            ),
            _ => return Err(MerkleDatabaseError::InvalidRecord),
        };

        let value = match val {
            Some(Value::Bytes(Bytes::Bytes(bytes))) => Some(bytes),
            Some(Value::Null) => None,
            _ => return Err(MerkleDatabaseError::InvalidRecord),
        };

        let children = match children_raw {
            Some(Value::Map(mut child_map)) => {
                let mut result = BTreeMap::new();
                for (k, v) in child_map {
                    result.insert(key_to_string(k)?, text_to_string(v)?);
                }
                result
            }
            None => BTreeMap::new(),
            _ => return Err(MerkleDatabaseError::InvalidRecord),
        };

        Ok(Node { value, children })
    }
}

/// Converts a CBOR Key to its String content
fn key_to_string(key_val: Key) -> Result<String, MerkleDatabaseError> {
    match key_val {
        Key::Text(Text::Text(s)) => Ok(s),
        _ => Err(MerkleDatabaseError::InvalidRecord),
    }
}

/// Converts a CBOR Text Value to its String content
fn text_to_string(text_val: Value) -> Result<String, MerkleDatabaseError> {
    match text_val {
        Value::Text(Text::Text(s)) => Ok(s),
        _ => Err(MerkleDatabaseError::InvalidRecord),
    }
}

/// Creates a hash of the given bytes
fn hash(input: &[u8]) -> String {
    let mut sha = Sha512::new();
    sha.input(input);
    String::from(&(sha.result_str())[..64])
}

#[cfg(test)]
mod tests {
    use super::*;
    use database::database::DatabaseError;
    use database::lmdb::LmdbContext;
    use database::lmdb::LmdbDatabase;

    use std::env;
    use std::fs::remove_file;
    use std::path::Path;
    use std::panic;
    use std::str::from_utf8;
    use std::thread;
    use rand::{seq, thread_rng};

    #[test]
    fn node_serialize() {
        let n = Node {
            value: Some(b"hello".to_vec()),
            children: vec![("ab".to_string(), "123".to_string())]
                .into_iter()
                .collect(),
        };

        let packed = n.into_bytes()
            .unwrap()
            .iter()
            .map(|b| format!("{:x}", b))
            .collect::<Vec<_>>()
            .join("");
        // This expected output was generated using the python structures
        let output = "a26163a16261626331323361764568656c6c6f";

        assert_eq!(output, packed);
    }

    #[test]
    fn node_deserialize() {
        let packed = from_hex("a26163a162303063616263617647676f6f64627965");

        let unpacked = Node::from_bytes(&packed).unwrap();
        assert_eq!(
            Node {
                value: Some(b"goodbye".to_vec()),
                children: vec![("00".to_string(), "abc".to_string())]
                    .into_iter()
                    .collect(),
            },
            unpacked
        );
    }

    #[test]
    fn node_roundtrip() {
        let n = Node {
            value: Some(b"hello".to_vec()),
            children: vec![("ab".to_string(), "123".to_string())]
                .into_iter()
                .collect(),
        };

        let packed = n.into_bytes().unwrap();
        let unpacked = Node::from_bytes(&packed).unwrap();

        assert_eq!(
            Node {
                value: Some(b"hello".to_vec()),
                children: vec![("ab".to_string(), "123".to_string())]
                    .into_iter()
                    .collect(),
            },
            unpacked
        )
    }

    #[test]
    fn merkle_trie_root_advance() {
        run_test(|merkle_path| {
            let mut merkle_db = make_db(&merkle_path);

            let orig_root = merkle_db.get_merkle_root();
            let new_root = merkle_db.set("abcd", "data_value".as_bytes()).unwrap();

            assert_eq!(merkle_db.get_merkle_root(), orig_root, "Incorrect root");
            assert_ne!(orig_root, new_root, "root was not changed");
            assert!(
                !merkle_db.contains("abcd").unwrap(),
                "Should not contain the value"
            );

            merkle_db.set_merkle_root(new_root.clone()).unwrap();
            assert_eq!(merkle_db.get_merkle_root(), new_root, "Incorrect root");

            assert_value_at_address(&merkle_db, "abcd", "data_value");
        })
    }

    #[test]
    fn merkle_trie_delete() {
        run_test(|merkle_path| {
            let mut merkle_db = make_db(&merkle_path);

            let new_root = merkle_db.set("1234", "deletable".as_bytes()).unwrap();
            merkle_db.set_merkle_root(new_root).unwrap();
            assert_value_at_address(&merkle_db, "1234", "deletable");

            // deleting an unknown key should return an error
            assert!(merkle_db.delete("barf").is_err());

            let del_root = merkle_db.delete("1234").unwrap();

            // del_root hasn't been set yet, so address should still have value
            assert_value_at_address(&merkle_db, "1234", "deletable");
            merkle_db.set_merkle_root(del_root).unwrap();
            assert!(!merkle_db.contains("1234").unwrap());
        })
    }

    #[test]
    fn merkle_trie_update() {
        run_test(|merkle_path| {
            let mut merkle_db = make_db(&merkle_path);
            let init_root = merkle_db.get_merkle_root();

            let key_hashes = (0..1000)
                .map(|i| {
                    let key = format!("{:016x}", i);
                    let hash = hash(key.as_bytes());
                    (key, hash)
                })
                .collect::<Vec<_>>();

            let mut values = HashMap::new();
            for &(ref key, ref hashed) in key_hashes.iter() {
                let new_root = merkle_db.set(&hashed, key.as_bytes()).unwrap();
                values.insert(hashed.clone(), key.to_string());
                merkle_db.set_merkle_root(new_root).unwrap();
            }

            assert_ne!(init_root, merkle_db.get_merkle_root());

            let mut rng = thread_rng();
            let mut set_items = HashMap::new();
            // Perform some updates on the lower keys
            for i in seq::sample_iter(&mut rng, 0..500, 50).unwrap() {
                let hash_key = hash(format!("{:016x}", i).as_bytes());
                set_items.insert(hash_key.clone(), "5.0".as_bytes().to_vec());
                values.insert(hash_key.clone(), "5.0".to_string());
            }

            // perform some deletions on the upper keys
            let delete_items = seq::sample_iter(&mut rng, 500..1000, 50)
                .unwrap()
                .into_iter()
                .map(|i| hash(format!("{:016x}", i).as_bytes()))
                .collect::<Vec<String>>();

            for hash in delete_items.iter() {
                values.remove(hash);
            }

            let virtual_root = merkle_db.update(&set_items, &delete_items, true).unwrap();

            // virtual root shouldn't match actual contents of tree
            assert!(merkle_db.set_merkle_root(virtual_root.clone()).is_err());

            let actual_root = merkle_db.update(&set_items, &delete_items, false).unwrap();
            // the virtual root should be the same as the actual root
            assert_eq!(virtual_root, actual_root);
            assert_ne!(actual_root, merkle_db.get_merkle_root());

            merkle_db.set_merkle_root(actual_root).unwrap();

            for (address, value) in values {
                assert_value_at_address(&merkle_db, &address, &value);
            }

            for address in delete_items {
                assert!(merkle_db.get(&address).is_err());
            }
        })
    }

    #[test]
    fn leaf_iteration() {
        run_test(|merkle_path| {
            let mut merkle_db = make_db(merkle_path);

            {
                let mut leaf_iter = merkle_db.leaves(None).unwrap();
                assert!(
                    leaf_iter.next().is_none(),
                    "Empty tree should return no leaves"
                );
            }

            let addresses = vec!["ab0000", "aba001", "abff02"];
            for (i, key) in addresses.iter().enumerate() {
                let new_root = merkle_db
                    .set(key, format!("{:04x}", i * 10).as_bytes())
                    .unwrap();
                merkle_db.set_merkle_root(new_root).unwrap();
            }

            assert_value_at_address(&merkle_db, "ab0000", "0000");
            assert_value_at_address(&merkle_db, "aba001", "000a");
            assert_value_at_address(&merkle_db, "abff02", "0014");

            let mut leaf_iter = merkle_db.leaves(None).unwrap();

            assert_eq!(
                ("ab0000".into(), "0000".as_bytes().to_vec()),
                leaf_iter.next().unwrap().unwrap()
            );
            assert_eq!(
                ("aba001".into(), "000a".as_bytes().to_vec()),
                leaf_iter.next().unwrap().unwrap()
            );
            assert_eq!(
                ("abff02".into(), "0014".as_bytes().to_vec()),
                leaf_iter.next().unwrap().unwrap()
            );
            assert!(leaf_iter.next().is_none(), "Iterator should be Exhausted");

            // test that we can start from an prefix:
            let mut leaf_iter = merkle_db.leaves(Some("abff")).unwrap();
            assert_eq!(
                ("abff02".into(), "0014".as_bytes().to_vec()),
                leaf_iter.next().unwrap().unwrap()
            );
            assert!(leaf_iter.next().is_none(), "Iterator should be Exhausted");
        })
    }

    fn run_test<T>(test: T) -> ()
    where
        T: FnOnce(&str) -> () + panic::UnwindSafe,
    {
        let dbpath = temp_db_path();

        let testpath = dbpath.clone();
        let result = panic::catch_unwind(move || test(&testpath));

        remove_file(dbpath).unwrap();

        assert!(result.is_ok())
    }

    fn assert_value_at_address(merkle_db: &MerkleDatabase, address: &str, expected_value: &str) {
        let value = merkle_db.get(address);
        assert!(value.is_ok(), format!("Value not returned: {:?}", value));
        assert_eq!(Ok(expected_value), from_utf8(&value.unwrap().unwrap()));
    }

    fn make_db(merkle_path: &str) -> MerkleDatabase {
        let ctx = LmdbContext::new(Path::new(merkle_path), 1, Some(120 * 1024 * 1024))
            .map_err(|err| DatabaseError::InitError(format!("{}", err)))
            .unwrap();
        let lmdb_db = LmdbDatabase::new(ctx, &[])
            .map_err(|err| DatabaseError::InitError(format!("{}", err)))
            .unwrap();
        MerkleDatabase::new(lmdb_db, None).unwrap()
    }

    fn temp_db_path() -> String {
        let mut temp_dir = env::temp_dir();

        let thread_id = thread::current().id();
        temp_dir.push(format!("merkle-{:?}.lmdb", thread_id));
        temp_dir.to_str().unwrap().to_string()
    }

    fn from_hex(s: &str) -> Vec<u8> {
        assert!(s.len() % 2 == 0);
        let mut res = Vec::with_capacity(s.len() / 2);
        let mut buf = 0;

        for (i, b) in s.bytes().enumerate() {
            buf <<= 4;
            match b {
                b'A'...b'F' => buf |= b - b'A' + 10,
                b'a'...b'f' => buf |= b - b'a' + 10,
                b'0'...b'9' => buf |= b - b'0',
                _ => continue,
            }

            if (i + 1) % 2 == 0 {
                res.push(buf);
            }
        }

        res
    }
}
