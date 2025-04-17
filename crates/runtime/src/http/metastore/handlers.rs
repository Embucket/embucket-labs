// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::error::{MetastoreAPIError, MetastoreAPIResult};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use snafu::ResultExt;

#[allow(clippy::wildcard_imports)]
use embucket_metastore::{
    error::{self as metastore_error, MetastoreError},
    *,
};

use crate::http::state::AppState;
use embucket_utils::scan_iterator::ScanIterator;
use validator::Validate;

pub type RwObjectVec<T> = Vec<RwObject<T>>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryParameters {
    #[serde(default)]
    pub cascade: Option<bool>,
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_volumes(
    State(state): State<AppState>,
) -> MetastoreAPIResult<Json<RwObjectVec<Volume>>> {
    let volumes = state
        .metastore
        .iter_volumes()
        .collect()
        .await
        .map_err(|e| MetastoreAPIError(MetastoreError::UtilSlateDB { source: e }))?
        .iter()
        .map(|v| hide_sensitive(v.clone()))
        .collect();
    Ok(Json(volumes))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
) -> MetastoreAPIResult<Json<RwObject<Volume>>> {
    match state.metastore.get_volume(&volume_name).await {
        Ok(Some(volume)) => Ok(Json(hide_sensitive(volume))),
        Ok(None) => Err(MetastoreError::VolumeNotFound {
            volume: volume_name.clone(),
        }
        .into()),
        Err(e) => Err(e.into()),
    }
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_volume(
    State(state): State<AppState>,
    Json(volume): Json<Volume>,
) -> MetastoreAPIResult<Json<RwObject<Volume>>> {
    volume
        .validate()
        .context(metastore_error::ValidationSnafu)?;
    state
        .metastore
        .create_volume(&volume.ident.clone(), volume)
        .await
        .map_err(MetastoreAPIError)
        .map(|v| Json(hide_sensitive(v)))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
    Json(volume): Json<Volume>,
) -> MetastoreAPIResult<Json<RwObject<Volume>>> {
    volume
        .validate()
        .context(metastore_error::ValidationSnafu)?;
    state
        .metastore
        .update_volume(&volume_name, volume)
        .await
        .map_err(MetastoreAPIError)
        .map(|v| Json(hide_sensitive(v)))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_volume(
    State(state): State<AppState>,
    Query(query): Query<QueryParameters>,
    Path(volume_name): Path<String>,
) -> MetastoreAPIResult<()> {
    state
        .metastore
        .delete_volume(&volume_name, query.cascade.unwrap_or_default())
        .await
        .map_err(MetastoreAPIError)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_databases(
    State(state): State<AppState>,
) -> MetastoreAPIResult<Json<Vec<RwObject<Database>>>> {
    state
        .metastore
        .iter_databases()
        .collect()
        .await
        .map_err(|e| MetastoreAPIError(MetastoreError::UtilSlateDB { source: e }))
        .map(Json)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_database(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> MetastoreAPIResult<Json<RwObject<Database>>> {
    match state.metastore.get_database(&database_name).await {
        Ok(Some(db)) => Ok(Json(db)),
        Ok(None) => Err(MetastoreError::DatabaseNotFound {
            db: database_name.clone(),
        }
        .into()),
        Err(e) => Err(e.into()),
    }
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_database(
    State(state): State<AppState>,
    Json(database): Json<Database>,
) -> MetastoreAPIResult<Json<RwObject<Database>>> {
    database
        .validate()
        .context(metastore_error::ValidationSnafu)?;
    state
        .metastore
        .create_database(&database.ident.clone(), database)
        .await
        .map_err(MetastoreAPIError)
        .map(Json)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_database(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
    Json(database): Json<Database>,
) -> MetastoreAPIResult<Json<RwObject<Database>>> {
    database
        .validate()
        .context(metastore_error::ValidationSnafu)?;
    //TODO: Implement database renames
    state
        .metastore
        .update_database(&database_name, database)
        .await
        .map_err(MetastoreAPIError)
        .map(Json)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_database(
    State(state): State<AppState>,
    Query(query): Query<QueryParameters>,
    Path(database_name): Path<String>,
) -> MetastoreAPIResult<()> {
    state
        .metastore
        .delete_database(&database_name, query.cascade.unwrap_or_default())
        .await
        .map_err(MetastoreAPIError)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_schemas(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> MetastoreAPIResult<Json<Vec<RwObject<Schema>>>> {
    state
        .metastore
        .iter_schemas(&database_name)
        .collect()
        .await
        .map_err(|e| MetastoreAPIError(MetastoreError::UtilSlateDB { source: e }))
        .map(Json)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_schema(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> MetastoreAPIResult<Json<RwObject<Schema>>> {
    let schema_ident = SchemaIdent {
        database: database_name.clone(),
        schema: schema_name.clone(),
    };
    match state.metastore.get_schema(&schema_ident).await {
        Ok(Some(schema)) => Ok(Json(schema)),
        Ok(None) => Err(MetastoreError::SchemaNotFound {
            db: database_name.clone(),
            schema: schema_name.clone(),
        }
        .into()),
        Err(e) => Err(e.into()),
    }
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_schema(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
    Json(schema): Json<Schema>,
) -> MetastoreAPIResult<Json<RwObject<Schema>>> {
    state
        .metastore
        .create_schema(&schema.ident.clone(), schema)
        .await
        .map_err(MetastoreAPIError)
        .map(Json)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_schema(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
    Json(schema): Json<Schema>,
) -> MetastoreAPIResult<Json<RwObject<Schema>>> {
    let schema_ident = SchemaIdent::new(database_name, schema_name);
    // TODO: Implement schema renames
    state
        .metastore
        .update_schema(&schema_ident, schema)
        .await
        .map_err(MetastoreAPIError)
        .map(Json)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_schema(
    State(state): State<AppState>,
    Query(query): Query<QueryParameters>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> MetastoreAPIResult<()> {
    let schema_ident = SchemaIdent::new(database_name, schema_name);
    state
        .metastore
        .delete_schema(&schema_ident, query.cascade.unwrap_or_default())
        .await
        .map_err(MetastoreAPIError)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_tables(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> MetastoreAPIResult<Json<Vec<RwObject<Table>>>> {
    let schema_ident = SchemaIdent::new(database_name, schema_name);
    state
        .metastore
        .iter_tables(&schema_ident)
        .collect()
        .await
        .map_err(|e| MetastoreAPIError(MetastoreError::UtilSlateDB { source: e }))
        .map(Json)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_table(
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
) -> MetastoreAPIResult<Json<RwObject<Table>>> {
    let table_ident = TableIdent::new(&database_name, &schema_name, &table_name);
    match state.metastore.get_table(&table_ident).await {
        Ok(Some(table)) => Ok(Json(table)),
        Ok(None) => Err(MetastoreError::TableNotFound {
            db: database_name.clone(),
            schema: schema_name.clone(),
            table: table_name.clone(),
        }
        .into()),
        Err(e) => Err(e.into()),
    }
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_table(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
    Json(table): Json<TableCreateRequest>,
) -> MetastoreAPIResult<Json<RwObject<Table>>> {
    table.validate().context(metastore_error::ValidationSnafu)?;
    let table_ident = TableIdent::new(&database_name, &schema_name, &table.ident.table);
    state
        .metastore
        .create_table(&table_ident, table)
        .await
        .map_err(MetastoreAPIError)
        .map(Json)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_table(
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
    Json(table): Json<TableUpdate>,
) -> MetastoreAPIResult<Json<RwObject<Table>>> {
    let table_ident = TableIdent::new(&database_name, &schema_name, &table_name);
    state
        .metastore
        .update_table(&table_ident, table)
        .await
        .map_err(MetastoreAPIError)
        .map(Json)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_table(
    State(state): State<AppState>,
    Query(query): Query<QueryParameters>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
) -> MetastoreAPIResult<()> {
    let table_ident = TableIdent::new(&database_name, &schema_name, &table_name);
    state
        .metastore
        .delete_table(&table_ident, query.cascade.unwrap_or_default())
        .await
        .map_err(MetastoreAPIError)
}

#[allow(clippy::needless_pass_by_value)]
#[must_use]
pub fn hide_sensitive(volume: RwObject<Volume>) -> RwObject<Volume> {
    let mut new_volume = volume;
    if let VolumeType::S3(ref mut s3_volume) = new_volume.data.volume {
        if let Some(AwsCredentials::AccessKey(ref mut access_key)) = s3_volume.credentials {
            access_key.aws_access_key_id = "******".to_string();
            access_key.aws_secret_access_key = "******".to_string();
        }
    }
    new_volume
}
