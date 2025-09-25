pub mod client;
pub mod snow_sql;
pub mod sql_macro;
pub mod test_rest_quick_sqls;

#[cfg(not(feature = "external-server"))]
pub mod test_gzip_encoding;

#[cfg(not(feature = "external-server"))]
pub mod test_generic_sqls;

#[cfg(not(feature = "external-server"))]
pub mod test_abort_by_request_id;

#[cfg(feature = "external-server")]
pub mod external_server;
