use snafu::{Snafu, ResultExt};
use api_structs::{auth::AuthResponse, volumes::{Volume, VolumeCreatePayload}};

use crate::requests::requests::AuthenticatedRequests;
use crate::requests::error::HttpRequestError;
// use api_structs::databases::DatabaseCreatePayload;

pub type ApiClientResult<T> = Result<T, HttpRequestError>;

pub struct Database {
    pub requests: Box<dyn AuthenticatedRequests>,
}

#[async_trait::async_trait]
pub trait DatabaseApi {
    async fn login(&mut self, user: String, password: String) -> ApiClientResult<AuthResponse>;
    async fn create_volume(&self, volume: Volume) -> ApiClientResult<()>;
    async fn create_database(&self) -> ApiClientResult<()>;
    async fn create_schema(&self) -> ApiClientResult<()>;
    async fn create_table(&self) -> ApiClientResult<()>;
}

impl DatabaseApi for Database {
    async fn login(&mut self, user: String, password: String) -> ApiClientResult<AuthResponse> {
        self.requests.login(user, password).await
    }

    async fn create_volume(&self, volume: Volume) -> ApiClientResult<()> {
        self.requests.generic_request::<VolumeCreatePayload, VolumeCreateResponse>(
            Method::POST, "/ui/volumes",
            &VolumeCreatePayload { data: volume },
        ).await?;
        Ok(())
    }

    async fn create_database(&self) -> ApiClientResult<()> {
        // self.metastore.create_database(name, database)
        Ok(())
    }

    async fn create_schema(&self) -> ApiClientResult<()> {
        Ok(())
    }

    async fn create_table(&self) -> ApiClientResult<()> {
        Ok(())
    }
}
