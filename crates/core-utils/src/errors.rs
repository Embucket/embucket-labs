use bytes::Bytes;
use snafu::Location;
use snafu::prelude::*;
use std::fmt::Debug;
use rusqlite;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("SlateDB error: {error}"))]
    Database {
        #[snafu(source)]
        error: slatedb::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("SlateDB error while fetching key {key}: {error}"))]
    KeyGet {
        key: String,
        #[snafu(source)]
        error: slatedb::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("SlateDB error while deleting key {key}: {error}"))]
    KeyDelete {
        key: String,
        #[snafu(source)]
        error: slatedb::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("SlateDB error while putting key {key}: {error}"))]
    KeyPut {
        key: String,
        #[snafu(source)]
        error: slatedb::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error serializing value: {error}"))]
    SerializeValue {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Deserialize error: {error}, key: {key:?}"))]
    DeserializeValue {
        #[snafu(source)]
        error: serde_json::Error,
        key: Bytes,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Key Not found"))]
    KeyNotFound {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Scan Failed: {error}"))]
    ScanFailed {
        #[snafu(source)]
        error: slatedb::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Sqlite error: {error}"))]
    RuSqlite {
        #[snafu(source)]
        error: rusqlite::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error creating sqlite schema: {error}"))]
    CoreSqlite {
        #[snafu(source)]
        error: core_sqlite::Error,
        #[snafu(implicit)]
        location: Location,
    },

    // #[snafu(display("Sqlite connector error: {error}"))]
    // Connector {
    //     #[snafu(source(from(deadpool_sqlite::InteractError, Box::new)))]
    //     error: Box<deadpool_sqlite::InteractError>,
    //     #[snafu(implicit)]
    //     location: Location,
    // }
}
