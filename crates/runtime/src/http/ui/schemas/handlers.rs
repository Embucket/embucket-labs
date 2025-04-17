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
use crate::http::ui::schemas::models::SchemasParameters;
use crate::http::{
    error::ErrorResponse,
    metastore::handlers::QueryParameters,
    ui::schemas::error::{SchemasAPIError, SchemasResult},
    ui::schemas::models::{
        Schema, SchemaCreatePayload, SchemaCreateResponse, SchemaResponse, SchemaUpdatePayload,
        SchemaUpdateResponse, SchemasResponse,
    },
};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use embucket_metastore::error::MetastoreError;
use embucket_metastore::models::SchemaIdent as MetastoreSchemaIdent;
use embucket_metastore::Schema as MetastoreSchema;
use embucket_utils::scan_iterator::ScanIterator;
use std::collections::HashMap;
use std::convert::From;
use std::convert::Into;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_schema,
        delete_schema,
        // update_schema,
        // get_schema,
        list_schemas,
    ),
    components(
        schemas(
            SchemaCreatePayload,
            SchemaCreateResponse,
            SchemasResponse,
            Schema,
            ErrorResponse,
        )
    ),
    tags(
        (name = "schemas", description = "Schemas endpoints")
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
    request_body = SchemaCreatePayload,
    responses(
        (status = 200, description = "Successful Response", body = SchemaCreateResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_schema(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
    Json(payload): Json<SchemaCreatePayload>,
) -> SchemasResult<Json<SchemaCreateResponse>> {
    let ident = MetastoreSchemaIdent::new(database_name, payload.name);
    let schema = MetastoreSchema {
        ident,
        properties: Some(HashMap::new()),
    };
    state
        .metastore
        .create_schema(&schema.ident.clone(), schema)
        .await
        .map_err(|e| SchemasAPIError::Create { source: e })
        .map(|rw_object| {
            Json(SchemaCreateResponse {
                data: Schema::from(rw_object),
            })
        })
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
    let ident = MetastoreSchemaIdent::new(database_name, schema_name);
    state
        .metastore
        .delete_schema(&ident, query.cascade.unwrap_or_default())
        .await
        .map_err(|e| SchemasAPIError::Delete { source: e })
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
    let schema_ident = MetastoreSchemaIdent {
        database: database_name.clone(),
        schema: schema_name.clone(),
    };
    match state.metastore.get_schema(&schema_ident).await {
        Ok(Some(rw_object)) => Ok(Json(SchemaResponse {
            data: Schema::from(rw_object),
        })),
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
    put,
    operation_id = "updateSchema",
    path="/ui/databases/{databaseName}/schemas/{schemaName}",
    tags = ["schemas"],
    params(
        ("databaseName" = String, description = "Database Name"),
        ("schemaName" = String, description = "Schema Name")
    ),
    request_body = SchemaUpdatePayload,
    responses(
        (status = 200, body = SchemaUpdateResponse),
        (status = 404, description = "Schema not found"),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_schema(
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
    Json(schema): Json<SchemaUpdatePayload>,
) -> SchemasResult<Json<SchemaUpdateResponse>> {
    let schema_ident = MetastoreSchemaIdent::new(database_name, schema_name);
    // TODO: Implement schema renames
    state
        .metastore
        .update_schema(&schema_ident, schema.data.into())
        .await
        .map_err(|e| SchemasAPIError::Update { source: e })
        .map(|rw_object| {
            Json(SchemaUpdateResponse {
                data: Schema::from(rw_object),
            })
        })
}

#[utoipa::path(
    get,
    operation_id = "getSchemas",
    path="/ui/databases/{databaseName}/schemas",
    tags = ["schemas"],
    params(
        ("databaseName" = String, description = "Database Name"),
        ("cursor" = Option<String>, Query, description = "Schemas cursor"),
        ("limit" = Option<usize>, Query, description = "Schemas limit"),
        ("search" = Option<String>, Query, description = "Schemas search (start with)"),
    ),
    responses(
        (status = 200, body = SchemasResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_schemas(
    Query(parameters): Query<SchemasParameters>,
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> SchemasResult<Json<SchemasResponse>> {
    state
        .metastore
        .iter_schemas(&database_name)
        .cursor(parameters.cursor.clone())
        .limit(parameters.limit)
        .token(parameters.search)
        .collect()
        .await
        .map_err(|e| SchemasAPIError::List {
            source: MetastoreError::UtilSlateDB { source: e },
        })
        .map(|rw_objects| {
            let next_cursor = rw_objects
                .iter()
                .last()
                .map_or(String::new(), |rw_object| rw_object.ident.schema.clone());
            Json(SchemasResponse {
                items: rw_objects.into_iter().map(Schema::from).collect(),
                current_cursor: parameters.cursor,
                next_cursor,
            })
        })
}
