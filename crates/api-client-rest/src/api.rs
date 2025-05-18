use snafu::{Snafu, ResultExt};
use api_structs::{auth::AuthResponse, volumes::{Volume, VolumeCreatePayload}};

use crate::requests::query::AuthenticatedQueryRequest;
use crate::requests::error::QueryRequestError;
// use api_structs::databases::DatabaseCreatePayload;

#[derive(Snafu, Debug)]
pub enum DatabaseApiError {
    #[snafu(display("Database API error: {source}"))]
    Api{source: QueryRequestError},
}

pub type DatabaseApiResult<T> = Result<T, DatabaseApiError>;

pub struct Database {
    pub query: Box<dyn AuthenticatedQueryRequest>,
}

#[async_trait::async_trait]
pub trait DatabaseApi {
    async fn login(&mut self, user: String, password: String) -> DatabaseApiResult<AuthResponse>;
    async fn create_volume(&self, volume: Volume) -> DatabaseApiResult<()>;
    async fn create_database(&self) -> DatabaseApiResult<()>;
    async fn create_schema(&self) -> DatabaseApiResult<()>;
    async fn create_table(&self) -> DatabaseApiResult<()>;
}

impl DatabaseApi for Database {
    async fn login(&mut self, user: String, password: String) -> DatabaseApiResult<AuthResponse> {
        self.query.login(user, password).await.context(ApiSnafu)
    }

    fn create_volume(&self, volume: Volume) -> DatabaseApiResult<()> {
        self.query.query(query)
        // VolumeCreatePayload {
        //     data: volume,
        // };
        // self.metastore.create_volume(name, volume)
        // self.seed_data.volume_name
        Ok(())
    }

    fn create_database(&self) -> DatabaseApiResult<()> {
        // self.metastore.create_database(name, database)
        Ok(())
    }

    fn create_schema(&self) -> DatabaseApiResult<()> {
        Ok(())
    }

    fn create_table(&self) -> DatabaseApiResult<()> {
        Ok(())
    }
}
