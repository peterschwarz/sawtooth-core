use std::collections::HashMap;
use std::fmt;
use std::path::Path;

use config;

#[derive(Debug, Clone)]
pub enum PeeringConfig {
    Static,
    Dynamic,
}

#[derive(Debug, Clone)]
pub enum SchedulerConfig {
    Serial,
    Parallel,
}

#[derive(Debug, Clone)]
pub enum RolesConfig {
    Challenge,
    Trust,
}

pub struct ValidatorConfig {
    pub bind_network: Option<String>,
    pub bind_component: Option<String>,
    pub endpoint: Option<String>,

    pub peering: Option<PeeringConfig>,
    pub seeds: Option<Vec<String>>, pub peers: Option<Vec<String>>,

    pub network_public_key: Option<Vec<u8>>,
    pub network_private_key: Option<Vec<u8>>,

    pub scheduler: Option<SchedulerConfig>,
    pub minimum_peer_connectivity: Option<usize>,
    pub maximum_peer_connectivity: Option<usize>,

    pub permissions: Option<String>,
    pub roles: Option<RolesConfig>,

    pub opentsdb_url: Option<String>,
    pub opentsdb_db: Option<String>,
    pub opentsdb_username: Option<String>,
    pub opentsdb_password: Option<String>,
}

impl ValidatorConfig {
    pub fn empty() -> Self {
        ValidatorConfig {
            bind_network: None,
            bind_component: None,
            endpoint: None,
            peering: None,
            seeds: None,
            peers: None,
            network_public_key: None,
            network_private_key: None,
            scheduler: None,
            minimum_peer_connectivity: None,
            maximum_peer_connectivity: None,
            permissions: None,
            roles: None,
            opentsdb_url: None,
            opentsdb_db: None,
            opentsdb_username: None,
            opentsdb_password: None,
        }
    }

    pub fn default() -> Self {
        ValidatorConfig {
            bind_network: Some("tcp://127.0.0.1:8800".to_string()),
            bind_component: Some("tcp://127.0.0.1:4004".to_string()),
            peering: Some(PeeringConfig::Static),
            scheduler: Some(SchedulerConfig::Serial),
            minimum_peer_connectivity: Some(3),
            maximum_peer_connectivity: Some(10),

            ..Self::empty()
        }
    }
}

impl fmt::Display for ValidatorConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
           "ValidatorConfig {{ \
                scheduler: {:?} \
                bind_network: {:?}, \
                bind_component: {:?}, \
                endpoint: {:?} \
                peering: {:?}, \
                peers: {:?}, \
                seeds: {:?}, \
                minimum_peer_connectivity: {:?}, \
                maximum_peer_connectivity: {:?}, \
           }}",
           self.scheduler,
           self.bind_network,
           self.bind_component,
           self.endpoint,
           self.peering,
           self.peers,
           self.seeds,
           self.minimum_peer_connectivity,
           self.maximum_peer_connectivity,
       )
    }
}

pub fn load_toml_validator_config(validator_toml_path: &Path)
    -> Result<ValidatorConfig, LocalConfigurationError>
{
    if !validator_toml_path.exists() {

        // Log
        return Ok(ValidatorConfig::empty())
    }

    let mut toml_config = config::Config::default();
    toml_config.merge(config::File::from(validator_toml_path))?;

    let mut config_map: HashMap<String, config::Value> = toml_config.try_into()?;

    // Validate that only the following keys are valid
    let valid_keys = vec!["bind", "endpoint", "peering", "seeds", "peers", "network_public_key",
         "network_private_key", "scheduler", "permissions", "roles",
         "opentsdb_url", "opentsdb_db", "opentsdb_username",
         "opentsdb_password", "minimum_peer_connectivity",
         "maximum_peer_connectivity"];

    for key in config_map.keys() {
        if !valid_keys.contains(&key.as_str()) {
            return Err(LocalConfigurationError::InvalidKey(
                    format!("Invalid key in validator config: {}", key)));
        }
    }

    // Extract the values
    let network_public_key = get_str(config_map.remove("network_private_key"))?
        .map(|s| s.into_bytes());
    let network_private_key = get_str(config_map.remove("network_private_key"))?
        .map(|s| s.into_bytes());

    let mut bind_network = None;
    let mut bind_component = None;
    if let Some(bindings) = get_str_array(config_map.remove("bind"))? {
        for bind in bindings {
            if bind.starts_with("network") {
                bind_network = Some(bind.split(":").skip(1).collect::<Vec<_>>().join(":"));
            }

            if bind.starts_with("component") {
                bind_component = Some(bind.split(":").skip(1).collect::<Vec<_>>().join(":"));
            }
        }
    }

    Ok(ValidatorConfig {
        bind_network,
        bind_component,

        scheduler: get_str(config_map.remove("scheduler"))?
            .map(|s| if s == "parallel" {
                SchedulerConfig::Parallel
            } else {
                SchedulerConfig::Serial
            }),

        endpoint: get_str(config_map.remove("endpoint"))?,
        peering: get_str(config_map.remove("peering"))?
            .map(|s| if s == "dynamic" {
                PeeringConfig::Dynamic 
            } else {
                PeeringConfig::Static
            }),
        seeds: get_str_array(config_map.remove("seeds"))?,
        peers: get_str_array(config_map.remove("peers"))?,

        minimum_peer_connectivity: get_usize(config_map.remove("minimum_peer_connectivity"))?,
        maximum_peer_connectivity: get_usize(config_map.remove("maximum_peer_connectivity"))?,

        network_public_key: network_public_key,
        network_private_key: network_private_key,

        // TODO
        permissions: None,
        roles: get_str(config_map.remove("roles"))?
            .map(|s| if s == "challenge" {
                RolesConfig::Challenge
            } else {
                RolesConfig::Trust
            }),

        opentsdb_url: get_str(config_map.remove("opentsdb_url"))?,
        opentsdb_db: get_str(config_map.remove("opentsdb_db"))?,
        opentsdb_username: get_str(config_map.remove("opentsdb_username"))?,
        opentsdb_password: get_str(config_map.remove("opentsdb_password"))?,
    })
}

pub fn merge_validator_config(configs: &mut [ValidatorConfig]) -> ValidatorConfig {
    let mut bind_network = None;
    let mut bind_component = None;
    let mut endpoint = None;
    let mut peering = None;
    let mut seeds = None;
    let mut peers = None;
    let mut network_public_key = None;
    let mut network_private_key = None;
    let mut scheduler = None;
    let mut permissions = None;
    let mut roles = None;
    let mut opentsdb_url = None;
    let mut opentsdb_db = None;
    let mut opentsdb_username = None;
    let mut opentsdb_password = None;
    let mut minimum_peer_connectivity = None;
    let mut maximum_peer_connectivity = None;

    configs.reverse();
    for config in configs {
        if config.bind_network.is_some() {
            bind_network = config.bind_network.clone();
        }
        if config.bind_component.is_some() {
            bind_component = config.bind_component.clone();
        }
        if config.endpoint.is_some() {
            endpoint = config.endpoint.clone();
        }
        if config.peering.is_some() {
            peering = config.peering.clone();
        }
        if config.seeds.is_some() {
            seeds = config.seeds.clone();
        }
        if config.peers.is_some() {
            peers = config.peers.clone();
        }
        if config.network_public_key.is_some() {
            network_public_key = config.network_public_key.clone();
        }
        if config.network_private_key.is_some() {
           network_private_key = config.network_private_key.clone();
        }
        if config.scheduler.is_some() {
            scheduler = config.scheduler.clone();
        }
        if config.permissions.is_some() {
            permissions = config.permissions.clone();
        }
        if config.roles.is_some() {
            roles = config.roles.clone();
        }
        if config.opentsdb_url.is_some() {
            opentsdb_url = config.opentsdb_url.clone();
        }
        if config.opentsdb_db.is_some() {
            opentsdb_db = config.opentsdb_db.clone();
        }
        if config.opentsdb_username.is_some() {
            opentsdb_username = config.opentsdb_username.clone();
        }
        if config.opentsdb_password.is_some() {
            opentsdb_password = config.opentsdb_password.clone();
        }
        if config.minimum_peer_connectivity.is_some() {
            minimum_peer_connectivity = config.minimum_peer_connectivity.clone();
        }
        if config.maximum_peer_connectivity.is_some() {
            maximum_peer_connectivity = config.maximum_peer_connectivity.clone();
        }
    }

    ValidatorConfig {
        bind_network,
        bind_component,
        endpoint,
        peering,
        seeds,
        peers,
        network_public_key,
        network_private_key,
        scheduler,
        permissions,
        roles,
        opentsdb_url,
        opentsdb_db,
        opentsdb_username,
        opentsdb_password,
        minimum_peer_connectivity,
        maximum_peer_connectivity,
    }
}

fn get_str(toml_val:  Option<config::Value>)
    -> Result<Option<String>, LocalConfigurationError>
{
    Ok(match toml_val {
        Some(val) => Some(val.into_str()?),
        None => None
    })
}

fn get_usize(toml_val:  Option<config::Value>)
    -> Result<Option<usize>, LocalConfigurationError>
{
    Ok(match toml_val {
        Some(val) => Some(val.into_int()? as usize),
        None => None
    })
} 

fn get_str_array(toml_val:  Option<config::Value>)
    -> Result<Option<Vec<String>>, LocalConfigurationError>
{
    match toml_val {
        Some(val) => {
            let (oks, mut fails): (Vec<Result<String, config::ConfigError>>,
                                   Vec<Result<String, config::ConfigError>>) = val.into_array()?
                .into_iter()
                .map(|v| v.into_str())
                .partition(Result::is_ok);

            if let Some(Err(err)) = fails.pop() {
                Err(LocalConfigurationError::from(err))
            } else {
                Ok(Some(oks.into_iter()
                     .map(|v| v.unwrap())
                     .collect()))
            }
        },
        None => Ok(None)
    }
}

#[derive(Debug)]
pub enum LocalConfigurationError {
    InvalidFile(config::ConfigError),
    InvalidConfig(config::ConfigError),
    InvalidKey(String),
    UnknownConfigurationError,
}

impl From<config::ConfigError> for LocalConfigurationError {
    fn from(error: config::ConfigError) -> Self {
        match error {
            config::ConfigError::NotFound(_)
                => LocalConfigurationError::InvalidFile(error),
            config::ConfigError::PathParse(_)
                => LocalConfigurationError::InvalidFile(error),
            config::ConfigError::FileParse{uri: _, cause: _}
                => LocalConfigurationError::InvalidFile(error),

            // Otherwise, invalid config
            _ => LocalConfigurationError::InvalidConfig(error)
        }
    }
}
