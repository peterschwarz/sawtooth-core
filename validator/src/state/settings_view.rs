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
use std::num::ParseIntError;

use crypto::digest::Digest;
use crypto::sha2::Sha256;
use protobuf;
use protobuf::Message;

use state::StateReader;
use state::StateDatabaseError;

use proto::setting::Setting;

const CONFIG_STATE_NAMESPACE: &str = "000000";
const MAX_KEY_PARTS: usize = 4;
const ADDRESS_PART_SIZE: usize = 16;

#[derive(Debug)]
pub enum SettingsViewError {
    StateDatabaseError(StateDatabaseError),
    EncodingError(protobuf::ProtobufError),

    ParseError(String),
    ParseIntError(ParseIntError),
    UnknownError,
}

impl From<StateDatabaseError> for SettingsViewError {
    fn from(err: StateDatabaseError) -> Self {
        SettingsViewError::StateDatabaseError(err)
    }
}

impl From<protobuf::ProtobufError> for SettingsViewError {
    fn from(err: protobuf::ProtobufError) -> Self {
        SettingsViewError::EncodingError(err)
    }
}

pub struct SettingsView<R>
where
    R: StateReader,
{
    state_reader: R,
}

impl<R> SettingsView<R>
where
    R: StateReader,
{
    pub fn new(state_reader: R) -> Self {
        SettingsView { state_reader }
    }

    pub fn get_setting_str(
        &self,
        key: &str,
        default_value: Option<String>,
    ) -> Result<Option<String>, SettingsViewError> {
        self.get_setting(key, default_value, |s: &str| Ok(s.to_string()))
    }

    pub fn get_setting<T, F>(
        &self,
        key: &str,
        default_value: Option<T>,
        value_parser: F,
    ) -> Result<Option<T>, SettingsViewError>
    where
        F: FnOnce(&str) -> Result<T, SettingsViewError>,
    {
        self.state_reader
            .get(&setting_address(key))
            .map_err(SettingsViewError::from)
            .and_then(|bytes_opt: Option<Vec<u8>>| {
                Ok(if let Some(bytes) = bytes_opt {
                    Some(protobuf::parse_from_bytes::<Setting>(&bytes)?)
                } else {
                    None
                })
            })
            .and_then(|setting_opt: Option<Setting>| {
                if let Some(setting) = setting_opt {
                    for setting_entry in setting.get_entries() {
                        if setting_entry.get_key() == key {
                            let parsed_value = value_parser(&setting_entry.get_value())?;
                            return Ok(Some(parsed_value));
                        }
                    }
                }
                Ok(default_value)
            })
    }
}

fn setting_address(key: &str) -> String {
    let mut address = String::new();
    address.push_str(CONFIG_STATE_NAMESPACE);
    address.push_str(&key.splitn(MAX_KEY_PARTS - 1, ".")
        .map(short_hash)
        .collect::<Vec<_>>()
        .join(""));

    address
}

fn short_hash(s: &str) -> String {
    let mut sha = Sha256::new();
    sha.input(s.as_bytes());
    sha.result_str()[..ADDRESS_PART_SIZE].to_string()
}
