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
use icebucket_metastore::{
    error::{self as metastore_error, MetastoreError},
    *,
};
//use super::models::*;

use validator::Validate;

use crate::http::state::AppState;

/*#[derive(OpenApi)]
#[openapi(
    paths(
        list_volumes,
        get_volume
    ),
    components(
        schemas(
            HTTPIceBucketVolume,
        ),
    )
)]
pub struct MetastoreApi;*/

pub type RwObjectVec<T> = Vec<RwObject<T>>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryParameters {
    #[serde(default)]
    pub cascade: Option<bool>,
}

/*#[utoipa::path(
    get,
    operation_id = "listVolumes",
    path="/volumes",
    responses(
        (status = StatusCode::OK, body = RwObjectVec<IceBucketVolume>),
        (status = "5XX", description = "server error"),
    )
)]*/
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_volumes(
    State(state): State<AppState>,
) -> MetastoreAPIResult<Json<RwObjectVec<IceBucketVolume>>> {
    let res = state
        .metastore
        .list_volumes()
        .await
        .map_err(MetastoreAPIError)
        .map(Json)?;
    Ok(res)
}

/*#[utoipa::path(
    get,
    operation_id = "getVolume",
    path="/volumes/{volumeName}",
    params(
        ("volumeName" = String, description = "Volume Name")
    ),
    responses(
        (status = StatusCode::OK, body = RwObject<IceBucketVolume>),
        (status = StatusCode::NOT_FOUND, description = "Volume not found", body = ErrorResponse),
        (status = "5XX", description = "server error", body = ErrorResponse),
    )
)]*/
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
) -> MetastoreAPIResult<Json<RwObject<IceBucketVolume>>> {
    match state.metastore.get_volume(&volume_name).await {
        Ok(Some(volume)) => Ok(Json(volume)),
        Ok(None) => Err(MetastoreError::VolumeNotFound {
            volume: volume_name.clone(),
        }
        .into()),
        Err(e) => Err(e.into()),
    }
}

/*#[utoipa::path(
    post,
    operation_id = "createVolume",
    path="/volumes",
    request_body = IceBucketVolume,
    responses(
        (status = 200, body = RwObject<IceBucketVolume>),
        (status = "5XX", description = "server error", body = ErrorResponse),
    )
)]*/
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_volume(
    State(state): State<AppState>,
    Json(volume): Json<IceBucketVolume>,
) -> MetastoreAPIResult<Json<RwObject<IceBucketVolume>>> {
    volume
        .validate()
        .context(metastore_error::ValidationSnafu)?;
    state
        .metastore
        .create_volume(&volume.ident.clone(), volume)
        .await
        .map_err(MetastoreAPIError)
        .map(Json)
}

/*#[utoipa::path(
    put,
    operation_id = "updateVolume",
    path="/volumes/{volumeName}",
    params(("volumeName" = String, description = "Volume Name")),
    request_body = IceBucketVolume,
    responses((status = 200, body = IceBucketVolume),
              (status = 404, description = "Volume not found")
    )
)]*/
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
    Json(volume): Json<IceBucketVolume>,
) -> MetastoreAPIResult<Json<RwObject<IceBucketVolume>>> {
    volume
        .validate()
        .context(metastore_error::ValidationSnafu)?;
    state
        .metastore
        .update_volume(&volume_name, volume)
        .await
        .map_err(MetastoreAPIError)
        .map(Json)
}

/*#[utoipa::path(
    delete,
    operation_id = "deleteVolume",
    path="/volumes/{volumeName}",
    params(("volumeName" = String, description = "Volume Name")),
    responses((status = 200), (status = 404, description = "Volume not found"))
)]*/
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

/*#[utoipa::path(
    get,
    operation_id = "listDatabases",
    path="/databases",
    responses((status = 200, body = RwObjectVec<IceBucketDatabase>))
)]*/
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_databases(
    State(state): State<AppState>,
) -> MetastoreAPIResult<Json<Vec<RwObject<IceBucketDatabase>>>> {
    state
        .metastore
        .list_databases()
        .await
        .map_err(MetastoreAPIError)
        .map(Json)
}

/*#[utoipa::path(
    get,
    operation_id = "getDatabase",
    path="/databases/{databaseName}",
    params(("databaseName" = String, description = "Database Name")),
    responses((status = 200, body = RwObject<IceBucketDatabase>),
              (status = 404, description = "Database not found")
    )
)]*/
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_database(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> MetastoreAPIResult<Json<RwObject<IceBucketDatabase>>> {
    match state.metastore.get_database(&database_name).await {
        Ok(Some(db)) => Ok(Json(db)),
        Ok(None) => Err(MetastoreError::DatabaseNotFound {
            db: database_name.clone(),
        }
        .into()),
        Err(e) => Err(e.into()),
    }
}

/*#[utoipa::path(
    post,
    operation_id = "createDatabase",
    path="/databases",
    request_body = IceBucketDatabase,
    responses((status = 200, body = RwObject<IceBucketDatabase>))
)]*/
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_database(
    State(state): State<AppState>,
    Json(database): Json<IceBucketDatabase>,
) -> MetastoreAPIResult<Json<RwObject<IceBucketDatabase>>> {
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

/*#[utoipa::path(
    put,
    operation_id = "updateDatabase",
    path="/databases/{databaseName}",
    params(("databaseName" = String, description = "Database Name")),
    request_body = IceBucketDatabase,
    responses((status = 200, body = RwObject<IceBucketDatabase>),
              (status = 404, description = "Database not found")
    )
)]*/
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_database(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
    Json(database): Json<IceBucketDatabase>,
) -> MetastoreAPIResult<Json<RwObject<IceBucketDatabase>>> {
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

/*#[utoipa::path(
    delete,
    operation_id = "deleteDatabase",
    path="/databases/{databaseName}",
    params(("databaseName" = String, description = "Database Name")),
    responses((status = 200))
)]*/
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

/*#[utoipa::path(
    get,
    operation_id = "listSchemas",
    path="/databases/{databaseName}/schemas",
    params(("databaseName" = String, description = "Database Name")),
    responses((status = 200, body = RwObjectVec<IceBucketSchema>),
              (status = 404, description = "Database not found")
    )
)]*/
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_schemas(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> MetastoreAPIResult<Json<Vec<RwObject<IceBucketSchema>>>> {
    state
        .metastore
        .list_schemas(&database_name)
        .await
        .map_err(MetastoreAPIError)
        .map(Json)
}

/*#[utoipa::path(
    get,
    operation_id = "getSchema",
    path="/databases/{databaseName}/schemas/{schemaName}",
    params(("databaseName" = String, description = "Database Name"),
            ("schemaName" = String, description = "Schema Name")
    ),
    responses((status = 200, body = RwObject<IceBucketSchema>),
              (status = 404, description = "Schema not found")
    )
)]*/
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_schema(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> MetastoreAPIResult<Json<RwObject<IceBucketSchema>>> {
    let schema_ident = IceBucketSchemaIdent {
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

/*#[utoipa::path(
    post,
    operation_id = "createSchema",
    path="/databases/{databaseName}/schemas",
    params(("databaseName" = String, description = "Database Name")),
    request_body = IceBucketSchema,
    responses((status = 200, body = RwObject<IceBucketSchema>))
)]*/
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_schema(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
    Json(schema): Json<IceBucketSchema>,
) -> MetastoreAPIResult<Json<RwObject<IceBucketSchema>>> {
    state
        .metastore
        .create_schema(&schema.ident.clone(), schema)
        .await
        .map_err(MetastoreAPIError)
        .map(Json)
}

/*#[utoipa::path(
    put,
    operation_id = "updateSchema",
    path="/databases/{databaseName}/schemas/{schemaName}",
    params(("databaseName" = String, description = "Database Name"),
            ("schemaName" = String, description = "Schema Name")
    ),
    request_body = IceBucketSchema,
    responses((status = 200, body = RwObject<IceBucketSchema>),
              (status = 404, description = "Schema not found")
    )
)]*/
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_schema(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
    Json(schema): Json<IceBucketSchema>,
) -> MetastoreAPIResult<Json<RwObject<IceBucketSchema>>> {
    let schema_ident = IceBucketSchemaIdent::new(database_name, schema_name);
    // TODO: Implement schema renames
    state
        .metastore
        .update_schema(&schema_ident, schema)
        .await
        .map_err(MetastoreAPIError)
        .map(Json)
}

/*#[utoipa::path(
    delete,
    operation_id = "deleteSchema",
    path="/databases/{databaseName}/schemas/{schemaName}",
    params(("databaseName" = String, description = "Database Name"),
            ("schemaName" = String, description = "Schema Name")
    ),
    responses((status = 200))
)]*/
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_schema(
    State(state): State<AppState>,
    Query(query): Query<QueryParameters>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> MetastoreAPIResult<()> {
    let schema_ident = IceBucketSchemaIdent::new(database_name, schema_name);
    state
        .metastore
        .delete_schema(&schema_ident, query.cascade.unwrap_or_default())
        .await
        .map_err(MetastoreAPIError)
}

/*#[utoipa::path(
    get,
    operation_id = "listTables",
    path="/databases/{databaseName}/schemas/{schemaName}/tables",
    params(("databaseName" = String, description = "Database Name"),
            ("schemaName" = String, description = "Schema Name")
    ),
    responses((status = 200, body = RwObjectVec<IceBucketTable>),
              (status = 404, description = "Schema not found")
    )
)]*/
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_tables(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> MetastoreAPIResult<Json<Vec<RwObject<IceBucketTable>>>> {
    let schema_ident = IceBucketSchemaIdent::new(database_name, schema_name);
    state
        .metastore
        .list_tables(&schema_ident)
        .await
        .map_err(MetastoreAPIError)
        .map(Json)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_table(
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
) -> MetastoreAPIResult<Json<RwObject<IceBucketTable>>> {
    let table_ident = IceBucketTableIdent::new(&database_name, &schema_name, &table_name);
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
    Json(table): Json<IceBucketTableCreateRequest>,
) -> MetastoreAPIResult<Json<RwObject<IceBucketTable>>> {
    table.validate().context(metastore_error::ValidationSnafu)?;
    let table_ident = IceBucketTableIdent::new(&database_name, &schema_name, &table.ident.table);
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
    Json(table): Json<IceBucketTableUpdate>,
) -> MetastoreAPIResult<Json<RwObject<IceBucketTable>>> {
    let table_ident = IceBucketTableIdent::new(&database_name, &schema_name, &table_name);
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
    let table_ident = IceBucketTableIdent::new(&database_name, &schema_name, &table_name);
    state
        .metastore
        .delete_table(&table_ident, query.cascade.unwrap_or_default())
        .await
        .map_err(MetastoreAPIError)
}
