extern crate clap;
extern crate pyo3;
extern crate config;

mod cli;
mod conf;

use std::path::{Path, PathBuf};

use clap::{Arg, App, ArgMatches};
use pyo3::{Python,
           PyResult,
           PyModule,
           ToPyPointer,
           ObjectProtocol};

use cli::error::CliError;

const DISTRIBUTION_NAME: &'static str = "sawtooth-validator";
const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
    if let Err(err) = run() {
        println!("Unable to start Sawtooth: {:?}", err);
    }
}

fn run() -> Result<(), CliError> {
    // let args = parse_args();

    let gil = Python::acquire_gil();
    let python = gil.python();

    let matches = parse_args();

    let py_sawtooth = load_py_module(python, "sawtooth_validator.server.cli")?;


    let mut config_path = PathBuf::new();
    config_path.push(matches.value_of("config_dir").unwrap_or(""));
    config_path.push("validator.toml");

    let config = load_validator_config(
        create_validator_config(&matches)?,
        matches.value_of("config_dir").unwrap_or(""))?;

    println!("config: {}", config);
    
    // match py_sawtooth.call("main", (env!("CARGO_PKG_NAME"), py_args), ()) {
    //     Ok(_) => println!("Exiting..."),
    //     Err(err) => {
    //         eprintln!("Exiting with error {:?}", err);
    //         err.print(python);
    //     },
    // };

    Ok(())
}

fn load_validator_config(first_config: conf::ValidatorConfig,
                         config_dir: &str)
    -> Result<conf::ValidatorConfig, CliError>
{
    let mut config_path = PathBuf::new();
    config_path.push(config_dir);
    config_path.push("validator.toml");

    let toml_config = conf::load_toml_validator_config(config_path.as_path())
        .map_err(CliError::ConfigurationError)?;

    Ok(conf::merge_validator_config(
            &mut [first_config, toml_config, conf::ValidatorConfig::default()]))
}


fn create_validator_config(arg_matches: &ArgMatches)
    -> Result<conf::ValidatorConfig, CliError>
{
    let mut bind_network = None;
    let mut bind_component = None;

    if let Some(bindings) = arg_matches.values_of("bind") {
        for bind in  bindings {
            if bind.starts_with("network") {
                bind_network = Some(bind.split(":").skip(1).collect::<Vec<_>>().join(":"));
            }

            if bind.starts_with("component") {
                bind_component = Some(bind.split(":").skip(1).collect::<Vec<_>>().join(":"));
            }
        }
    }

    Ok(conf::ValidatorConfig {
        bind_network,
        bind_component,
        endpoint: arg_matches.value_of("endpoint").map(String::from),

        scheduler: arg_matches.value_of("scheduler")
            .map(|s| if s == "parallel" {
                conf::SchedulerConfig::Parallel
            } else {
                conf::SchedulerConfig::Serial
            }),
        peering: arg_matches.value_of("peering")
            .map(|s| if s == "dynamic" {
                conf::PeeringConfig::Dynamic
            } else {
                conf::PeeringConfig::Static
            }),
        seeds: arg_matches.values_of("seeds").map(|vals| vals.map(String::from).collect()),
        peers: arg_matches.values_of("peers").map(|vals| vals.map(String::from).collect()),
        roles: arg_matches.value_of("network_auth")
            .map(|s| if s == "challenge" {
                conf::RolesConfig::Challenge
            } else {
                conf::RolesConfig::Trust
            }),
        opentsdb_url: arg_matches.value_of("opentsdb_url").map(String::from),
        opentsdb_db: arg_matches.value_of("opentsdb_db").map(String::from),
        minimum_peer_connectivity: usize_arg(&arg_matches, "minimum_peer_connectivity")?,
        maximum_peer_connectivity: usize_arg(&arg_matches, "maximum_peer_connectivity")?,

        ..conf::ValidatorConfig::empty()
    })
}

fn usize_arg(arg_matches: &ArgMatches, arg_name: &str) -> Result<Option<usize>, CliError> {
    Ok(match arg_matches.value_of(arg_name) {
        Some(val) => Some(val.parse()
                          .map_err(|_| CliError::ArgumentError(
                                  format!("{} must be a positive integer",
                                          arg_name)))?),
        None => None
    })
}

fn parse_args<'a>() -> ArgMatches<'a> {
    let app = App::new(DISTRIBUTION_NAME)
        .version(VERSION)
        .about("Configures and starts a Sawtooth validator.")
        .arg(Arg::with_name("config_dir")
             .long("config-dir")
             .takes_value(true)
             .help("specify the configuration directory"))
        .arg(Arg::with_name("bind")
             .short("B")
             .long("bind")
             .takes_value(true)
             .multiple(true)
             .help("set the URL for the network or validator \
                    component service endpoints with the format \
                    network:<endpoint> or component:<endpoint>. \
                    Use two --bind options to specify both \
                    endpoints."))
        .arg(Arg::with_name("peering")
             .short("P")
             .long("peering")
             .takes_value(true)
             .possible_values(&["static", "dynamic"])
             .help("determine peering type for the validator: \
                    'static' (must use --peers to list peers) or \
                    'dynamic' (processes any static peers first, \
                    then starts topology buildout)."))
        .arg(Arg::with_name("endpoint")
             .short("E")
             .long("endpoint")
             .takes_value(true)
             .help("specifies the advertised network endpoint URL"))
        .arg(Arg::with_name("seeds")
             .short("S")
             .long("seeds")
             .takes_value(true)
             .multiple(true)
             .help("provide URI(s) for the initial connection to \
                    the validator network, in the format \
                    tcp://<hostname>:<port>. Specify multiple URIs \
                    in a comma-separated list. Repeating the --seeds \
                    option is also accepted."))
        .arg(Arg::with_name("peers")
             .short("p")
             .long("peers")
             .takes_value(true)
             .multiple(true)
             .help("list static peers to attempt to connect to \
                    in the format tcp://<hostname>:<port>. Specify \
                    multiple peers in a comma-separated list. \
                    Repeating the --peers option is also accepted."))
        .arg(Arg::with_name("verbose")
             .short("v")
             .long("verbose")
             .multiple(true)
             .help("enable more verbose output to stderr"))
        .arg(Arg::with_name("scheduler")
             .long("scheduler")
             .takes_value(true)
             .possible_values(&["serial", "parallel"])
             .help("set scheduler type: serial or parallel"))
        .arg(Arg::with_name("network_auth")
             .long("network-auth")
             .takes_value(true)
             .possible_values(&["trust", "challenge"])
             .help("identify type of authorization required to join validator \
                    network."))
        .arg(Arg::with_name("opentsdb_url")
             .long("opentsdb-url")
             .takes_value(true)
             .help("specify host and port for Open TSDB database used for \
                    metrics"))
        .arg(Arg::with_name("opentsdb_db")
             .long("opentsdb-db")
             .takes_value(true)
             .help("specify name of database used for storing metrics"))
        .arg(Arg::with_name("minimum_peer_connectivity")
             .long("minimum-peer-connectivity")
             .takes_value(true)
             .help("set the minimum number of peers required before stopping \
                    peer search"))
        .arg(Arg::with_name("maximum_peer_connectivity")
             .long("maximum-peer-connectivity")
             .takes_value(true)
             .help("set the maximum number of peers to accept"));

    app.get_matches()
}

fn check_directory(path: &str, human_readable_name: &str) -> Result<(), CliError> {
    let p = Path::new(path);

    if !p.exists() {
        return Err(CliError::FileSystemError(
                format!("{} directory does not exist: {}",
                        human_readable_name,
                        path)))
    }

    if !p.is_dir() {
        return Err(CliError::FileSystemError(
                format!("{} directory is not a directory: {}",
                        human_readable_name,
                        path)))
    }

    if let Ok(metadata) = p.metadata() {
        if metadata.permissions().readonly() {
            return Err(CliError::FileSystemError(
                    format!("{} directory is not writable: {}",
                            human_readable_name,
                            path)))
        }
    } else {
        return Err(CliError::FileSystemError(
                    format!("{} director has no meta data: {}",
                            human_readable_name,
                            path)))
    }

    Ok(())
}

// Python Briging Functions
fn load_py_module<'p>(python: Python<'p>, module_name: &str) -> Result<&'p PyModule, CliError> {
    python.import("sawtooth_validator.server.cli")
        .map_err(|err| {
             let traceback: String = if let Some(traceback) = err.ptraceback {
                 match traceback.extract(python) {
                     Ok(s) => s,
                     Err(_) => return CliError::PythonSystemError
                 }
             } else {
                 String::from("<No Traceback>")
             };

             CliError::PythonLoadError(
                 format!("Unable to load {}\n{}",
                         module_name,
                         traceback))
        })
}
