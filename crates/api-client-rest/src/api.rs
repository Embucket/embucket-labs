use crate::requests::error::HttpRequestError;
use crate::requests::client::{AuthenticatedClient, AuthenticatedRequests};
use api_structs::{
    auth::AuthResponse,
    databases::{Database, DatabaseCreatePayload, DatabaseCreateResponse},
    schemas::{Schema, SchemaCreatePayload, SchemaCreateResponse},
    // tables::{TableUploadPayload, TableUploadResponse},
    volumes::{Volume, VolumeCreatePayload, VolumeCreateResponse},
};
use http::Method;
use std::net::SocketAddr;

pub type ApiClientResult<T> = Result<T, HttpRequestError>;

pub struct DatabaseClient {
    pub client: AuthenticatedClient,
}

#[async_trait::async_trait]
pub trait DatabaseClientApi {
    async fn login(&mut self, user: &str, password: &str) -> ApiClientResult<AuthResponse>;
    async fn create_volume(&mut self, volume: Volume) -> ApiClientResult<()>;
    async fn create_database(&mut self, volume: &str, database: &str) -> ApiClientResult<()>;
    async fn create_schema(&mut self, database: &str, schema: &str) -> ApiClientResult<()>;
    // async fn upload_to_table(&self, table_name: String, payload: TableUploadPayload) -> ApiClientResult<TableUploadResponse>;
}

impl DatabaseClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            client: AuthenticatedClient::new(addr),
        }
    }
}

#[async_trait::async_trait]
impl DatabaseClientApi for DatabaseClient {
    async fn login(&mut self, user: &str, password: &str) -> ApiClientResult<AuthResponse> {
        self.client.login(user, password).await
    }

    async fn create_volume(&mut self, volume: Volume) -> ApiClientResult<()> {
        self.client
            .generic_request::<VolumeCreatePayload, VolumeCreateResponse>(
                Method::POST,
                &format!("http://{}/ui/volumes", self.client.addr()),
                &VolumeCreatePayload { data: volume },
            )
            .await?;
        Ok(())
    }

    async fn create_database(&mut self, volume: &str, database: &str) -> ApiClientResult<()> {
        self.client
            .generic_request::<DatabaseCreatePayload, DatabaseCreateResponse>(
                Method::POST,
                &format!("http://{}/ui/databases", self.client.addr()),
                &DatabaseCreatePayload {
                    data: Database {
                        name: database.to_string(),
                        volume: volume.to_string(),
                    },
                },
            )
            .await?;
        Ok(())
    }

    async fn create_schema(&mut self, database: &str, schema: &str) -> ApiClientResult<()> {
        self.client
            .generic_request::<SchemaCreatePayload, SchemaCreateResponse>(
                Method::POST,
                &format!("http://{}/ui/databases/{database}/schemas", self.client.addr()),
                &SchemaCreatePayload {
                    name: schema.to_string(),
                },
            )
            .await?;
        Ok(())
    }

    // async fn upload_to_table(&self, database: &str, schema: &str, table: &str) -> ApiClientResult<TableUploadResponse> {
    //     self.client.generic_request::<TableUploadPayload, TableUploadResponse>(
    //         Method::POST, format!("/ui/databases/{database}/schemas/{schema}/tables/{table}/rows"),
    //         &TableUploadPayload { upload_file:  },
    //     ).await
    // }
}
