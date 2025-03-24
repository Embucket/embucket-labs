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

use crate::http::state::AppState;
use crate::http::{
    error::ErrorResponse,
    metastore::handlers::QueryParameters,
    ui::schemas::errors::{SchemasAPIError, SchemasResult},
    ui::schemas::models::{SchemaPayload, SchemaResponse, SchemasResponse},
};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use icebucket_metastore::error::MetastoreError;
use icebucket_metastore::models::IceBucketSchemaIdent;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_schema,
        delete_schema,
        get_schema,
        update_schema,
        list_schemas,
    ),
    components(
        schemas(
            SchemaPayload,
            SchemaResponse,
            SchemasResponse,
            ErrorResponse,
        )
    ),
    tags(
        (name = "schemas", description = "Schemas management endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    post,
    path = "/ui/databases/{databaseName}/schemas",
    operation_id = "createSchema",
    tags = ["schemas"],
    params(
        ("databaseName" = String, description = "Database Name")
    ),
    request_body = SchemaPayload,
    responses(
        (status = 200, description = "Successful Response", body = SchemaResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_schema(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
    Json(schema): Json<SchemaPayload>,
) -> SchemasResult<Json<SchemaResponse>> {
    state
        .metastore
        .create_schema(&schema.data.ident.clone(), schema.data)
        .await
        .map_err(|e| SchemasAPIError::Create { source: e })
        .map(|rw_object| {
            Json(SchemaResponse {
                data: rw_object.data,
            })
        })
}

#[utoipa::path(
    get,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}",
    params(
        ("databaseName" = String, description = "Database Name"),
        ("schemaName" = String, description = "Schema Name")
    ),
    operation_id = "getSchema",
    tags = ["schemas"],
    responses(
        (status = 200, description = "Successful Response", body = SchemaResponse),
        (status = 404, description = "Schema not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_schema(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> SchemasResult<Json<SchemaResponse>> {
    let schema_ident = IceBucketSchemaIdent {
        database: database_name.clone(),
        schema: schema_name.clone(),
    };
    match state.metastore.get_schema(&schema_ident).await {
        Ok(Some(schema)) => Ok(Json(SchemaResponse { data: schema.data })),
        Ok(None) => Err(SchemasAPIError::Get {
            source: MetastoreError::SchemaNotFound {
                db: database_name.clone(),
                schema: schema_name.clone(),
            },
        }),
        Err(e) => Err(SchemasAPIError::Get { source: e }),
    }
}

#[utoipa::path(
    delete,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}",
    operation_id = "deleteSchema",
    tags = ["schemas"],
    params(
        ("databaseName" = String, description = "Database Name"),
        ("schemaName" = String, description = "Schema Name")
    ),
    responses(
        (status = 204, description = "Successful Response"),
        (status = 404, description = "Schema not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_schema(
    State(state): State<AppState>,
    Query(query): Query<QueryParameters>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> SchemasResult<()> {
    let schema_ident = IceBucketSchemaIdent::new(database_name, schema_name);
    state
        .metastore
        .delete_schema(&schema_ident, query.cascade.unwrap_or_default())
        .await
        .map_err(|e| SchemasAPIError::Delete { source: e })
}

#[utoipa::path(
    put,
    operation_id = "updateSchema",
    path="/ui/databases/{databaseName}/schemas/{schemaName}",
    tags = ["schemas"],
    params(
        ("databaseName" = String, description = "Database Name"),
        ("schemaName" = String, description = "Schema Name")
    ),
    request_body = SchemaPayload,
    responses(
        (status = 200, body = SchemaResponse),
        (status = 404, description = "Schema not found"),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_schema(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
    Json(schema): Json<SchemaPayload>,
) -> SchemasResult<Json<SchemaResponse>> {
    let schema_ident = IceBucketSchemaIdent::new(database_name, schema_name);
    // TODO: Implement schema renames
    state
        .metastore
        .update_schema(&schema_ident, schema.data)
        .await
        .map_err(|e| SchemasAPIError::Update { source: e })
        .map(|rw_object| {
            Json(SchemaResponse {
                data: rw_object.data,
            })
        })
}

#[utoipa::path(
    get,
    operation_id = "getSchemas",
    path="/ui/databases/{databaseName}/schemas",
    tags = ["schemas"],
    params(
        ("databaseName" = String, description = "Database Name")
    ),
    responses(
        (status = 200, body = SchemasResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_schemas(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> SchemasResult<Json<SchemasResponse>> {
    state
        .metastore
        .list_schemas(&database_name)
        .await
        .map_err(|e| SchemasAPIError::List { source: e })
        .map(|rw_objects| {
            Json(SchemasResponse {
                items: rw_objects
                    .iter()
                    .map(|rw_object| rw_object.data.clone())
                    .collect(),
            })
        })
}
