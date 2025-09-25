pub mod client;
pub mod snow_sql;
pub mod sql_macro;
pub mod test_rest_quick_sqls;
cfg_if::cfg_if! {
    if #[cfg(feature = "default-server")] {
        pub mod test_gzip_encoding;
        pub mod test_generic_sqls;
        pub mod test_abort_by_request_id;
        pub use crate::server::test_server::run_test_rest_api_server;
    } else {
        pub mod external_server;
        pub use crate::tests::external_server::run_test_rest_api_server;
    }
}
