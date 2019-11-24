#![allow(non_snake_case)]

use clap::{App, Arg};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::json;
use serde_json::Value;
use unix_socket::UnixSeqpacket;

#[macro_use]
extern crate log;
#[macro_use]
extern crate cute;
extern crate pretty_env_logger;

fn main() {
    pretty_env_logger::init();
    debug!("KVStore Started");

    let args = App::new("CS3700 Project 5")
        .author("Nelson Chan <chan.chak@husky.neu.edu>")
        .arg(
            Arg::with_name("my ID")
                .index(1)
                .required(true)
                .help("ID of this replica"),
        )
        .arg(
            Arg::with_name("replica IDs")
                .index(2)
                .required(true)
                .multiple(true)
                .help("IDs of other replicas"),
        )
        .get_matches();

    let arg_my_ID = args.value_of("username").expect("ID of this replica not provided.");
    let arg_replica_IDs: Vec<_> = args.values_of("replica IDs").expect("replicas not provided").collect();

    debug!("my ID: {}, Replicas: {:?}", arg_my_ID, arg_replica_IDs);
}