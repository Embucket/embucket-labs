pub mod client;
pub mod snow_sql;
pub mod sql_macro;
pub mod test_rest_quick_sqls;

#[cfg(not(feature = "external-server"))]
pub mod test_gzip_encoding;

#[cfg(not(feature = "external-server"))]
pub mod test_generic_sqls;

#[cfg(feature = "external-server")]
pub mod external_server;
