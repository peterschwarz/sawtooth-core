extern crate clap;
extern crate pyo3;

use clap::{Arg, App, ArgMatches};
use pyo3::{Python, Py, PyList, PyString, PyResult, ObjectProtocol};

const DISTRIBUTION_NAME: &'static str = "sawtooth-validator";
const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
    // let args = parse_args();

    let gil = Python::acquire_gil();
    let python = gil.python();

    let args: Vec<String> = std::env::args().skip(1)
        .collect();
    
    let py_args = PyList::new(python, &args);
    let py_sawtooth = match python.import("sawtooth_validator.server.cli") {
        Ok(module) => module,
        Err(err) => {
            eprintln!("Unable to load sawtooth: {:?}", err);
            err.print(python);
            return;
        }
    };
    
    match py_sawtooth.call("main", (env!("CARGO_PKG_NAME"), py_args), ()) {
        Ok(_) => println!("Exiting..."),
        Err(err) => {
            eprintln!("Exiting with error {:?}", err);
            err.print(python);
        },
    };
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
