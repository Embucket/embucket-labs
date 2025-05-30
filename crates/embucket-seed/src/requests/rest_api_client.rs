//! Module containing the REST API client implementation for the Embucket service.
//!
//! This module provides a high-level client for performing operations on the Embucket API,
//! including managing volumes, databases, schemas, and tables.

use crate::external_models::{
    AuthResponse, DatabaseCreatePayload, DatabaseCreateResponse, QueryCreateResponse,
    SchemaCreatePayload, SchemaCreateResponse, VolumeCreatePayload, VolumeCreateResponse,
};
use crate::requests::error::HttpRequestResult;
use crate::requests::service_client::{BasicAuthClient, ServiceClient};
use http::Method;
use std::net::SocketAddr;

/// A client for interacting with the Embucket REST API.
///
/// This client provides methods for managing Embucket resources including volumes,
/// databases, schemas, and tables. It wraps a lower-level `ServiceClient` to handle
/// HTTP requests and authentication.
#[derive(Debug)]
pub struct RestClient {
    /// The underlying service client used for HTTP requests
    pub client: BasicAuthClient,
}

/// A trait defining the interface for REST API clients that interact with Embucket resources.
///
/// This trait provides methods for performing CRUD operations on Embucket resources
/// in a type-safe manner.
#[async_trait::async_trait]
pub trait RestApiClient {
    /// Authenticates with the Embucket service.
    ///
    /// # Arguments
    /// * `user` - The username for authentication
    /// * `password` - The password for authentication
    ///
    /// # Errors
    /// Returns `HttpRequestError` if authentication fails.
    async fn login(&mut self, user: &str, password: &str) -> HttpRequestResult<AuthResponse>;

    /// Creates a new volume in the Embucket service.
    ///
    /// # Errors
    /// Returns `HttpRequestError` if the operation fails.
    async fn create_volume(
        &mut self,
        volume: VolumeCreatePayload,
    ) -> HttpRequestResult<VolumeCreateResponse>;

    /// Creates a new database within a volume.
    ///
    /// # Arguments
    /// * `volume` - The name of the parent volume
    /// * `database` - The name of the database to create
    ///
    /// # Errors
    /// Returns `HttpRequestError` if the operation fails.
    async fn create_database(
        &mut self,
        volume: &str,
        database: &str,
    ) -> HttpRequestResult<DatabaseCreateResponse>;

    /// Creates a new schema within a database.
    ///
    /// # Arguments
    /// * `database` - The name of the parent database
    /// * `schema` - The name of the schema to create
    ///
    /// # Errors
    /// Returns `HttpRequestError` if the operation fails.
    async fn create_schema(
        &mut self,
        database: &str,
        schema: &str,
    ) -> HttpRequestResult<SchemaCreateResponse>;

    /// Creates a new table within a schema.
    ///
    /// # Arguments
    /// * `database` - The name of the parent database
    /// * `schema` - The name of the parent schema
    /// * `table` - The name of the table to create
    /// * `columns` - A slice of (column_name, column_type) tuples defining the table columns
    ///
    /// # Errors
    /// Returns `HttpRequestError` if the operation fails.
    async fn create_table(
        &mut self,
        database: &str,
        schema: &str,
        table: &str,
        columns: &[(String, String)],
    ) -> HttpRequestResult<QueryCreateResponse>;
    // async fn upload_to_table(&self, table_name: String, payload: TableUploadPayload) -> HttpRequestResult<TableUploadResponse>;
}

impl RestClient {
    /// Creates a new `RestClient` with the specified server address.
    #[must_use]
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            client: BasicAuthClient::new(addr),
        }
    }
}

#[async_trait::async_trait]
impl RestApiClient for RestClient {
    async fn login(&mut self, user: &str, password: &str) -> HttpRequestResult<AuthResponse> {
        self.client.login(user, password).await
    }

    async fn create_volume(
        &mut self,
        volume: VolumeCreatePayload,
    ) -> HttpRequestResult<VolumeCreateResponse> {
        Ok(self
            .client
            .generic_request::<VolumeCreatePayload, VolumeCreateResponse>(
                Method::POST,
                &format!("http://{}/ui/volumes", self.client.addr()),
                &volume,
            )
            .await?)
    }

    async fn create_database(
        &mut self,
        volume: &str,
        database: &str,
    ) -> HttpRequestResult<DatabaseCreateResponse> {
        Ok(self
            .client
            .generic_request::<DatabaseCreatePayload, DatabaseCreateResponse>(
                Method::POST,
                &format!("http://{}/ui/databases", self.client.addr()),
                &DatabaseCreatePayload {
                    name: database.to_string(),
                    volume: volume.to_string(),
                },
            )
            .await?)
    }

    async fn create_schema(
        &mut self,
        database: &str,
        schema: &str,
    ) -> HttpRequestResult<SchemaCreateResponse> {
        Ok(self
            .client
            .generic_request::<SchemaCreatePayload, SchemaCreateResponse>(
                Method::POST,
                &format!(
                    "http://{}/ui/databases/{database}/schemas",
                    self.client.addr()
                ),
                &SchemaCreatePayload {
                    name: schema.to_string(),
                },
            )
            .await?)
    }

    async fn create_table(
        &mut self,
        database: &str,
        schema: &str,
        table: &str,
        columns: &[(String, String)],
    ) -> HttpRequestResult<QueryCreateResponse> {
        let table_columns = columns
            .iter()
            .map(|(name, col_type)| format!("{name} {col_type}"))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(self
            .client
            .query(&format!(
                "CREATE TABLE {database}.{schema}.{table} ({table_columns});"
            ))
            .await?)
    }

    // async fn upload_to_table(&self, database: &str, schema: &str, table: &str) -> HttpRequestResult<TableUploadResponse> {
    //     self.client.generic_request::<TableUploadPayload, TableUploadResponse>(
    //         Method::POST, format!("/ui/databases/{database}/schemas/{schema}/tables/{table}/rows"),
    //         &TableUploadPayload { upload_file:  },
    //     ).await
    // }
}
