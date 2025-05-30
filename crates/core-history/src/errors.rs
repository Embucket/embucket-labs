use snafu::Snafu;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum HistoryStoreError {
    #[snafu(display("Error using key: {source}"))]
    BadKey { source: std::str::Utf8Error },

    #[snafu(display("Error adding worksheet: {source}"))]
    WorksheetAdd { source: core_utils::Error },

    #[snafu(display("Error getting worksheet: {source}"))]
    WorksheetGet { source: core_utils::Error },

    #[snafu(display("Error getting worksheets: {source}"))]
    WorksheetsList { source: core_utils::Error },

    #[snafu(display("Error deleting worksheet: {source}"))]
    WorksheetDelete { source: core_utils::Error },

    #[snafu(display("Error updating worksheet: {source}"))]
    WorksheetUpdate { source: core_utils::Error },

    #[snafu(display("Error adding query record: {source}"))]
    QueryAdd { source: core_utils::Error },

    #[snafu(display("Can't locate query record by key: {key}"))]
    QueryNotFound { key: String },

    #[snafu(display("Error adding query record reference: {source}"))]
    QueryReferenceAdd { source: core_utils::Error },

    #[snafu(display("Error getting query history: {source}"))]
    QueryGet { source: core_utils::Error },

    #[snafu(display("Can't locate worksheet by key: {message}"))]
    WorksheetNotFound { message: String },

    #[snafu(display("Bad query record reference key: {key}"))]
    QueryReferenceKey { key: String },

    #[snafu(display("Error getting worksheet queries: {source}"))]
    GetWorksheetQueries { source: core_utils::Error },

    #[snafu(display("Error adding query inverted key: {source}"))]
    QueryInvertedKeyAdd { source: core_utils::Error },

    #[snafu(display("Query item seek error: {source}"))]
    Seek { source: slatedb::SlateDBError },

    #[snafu(display("Deserialize error: {source}"))]
    DeserializeValue { source: serde_json::Error },
}
