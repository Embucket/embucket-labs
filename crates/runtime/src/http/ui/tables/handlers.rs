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

use crate::execution::query::IceBucketQueryContext;
use crate::http::error::ErrorResponse;
use crate::http::session::DFSessionId;
use crate::http::state::AppState;
use crate::http::ui::tables::error::{TablesAPIError, TablesResult};
use crate::http::ui::tables::models::{
    TableInfo, TableInfoColumn, TableInfoResponse, TablePreviewDataColumn,
    TablePreviewDataParameters, TablePreviewDataResponse, TablePreviewDataRow,
};
use arrow_array::{Array, StringArray};
use axum::extract::Query;
use axum::{
    extract::{Path, State},
    Json,
};
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        get_table_info,
        get_table_preview_data,
    ),
    components(
        schemas(
            TableInfoResponse,
            TableInfo,
            TableInfoColumn,
            TablePreviewDataResponse,
            TablePreviewDataColumn,
            TablePreviewDataRow,
            ErrorResponse,
        )
    ),
    tags(
        (name = "tables", description = "Tables endpoints.")
    )
)]
pub struct ApiDoc;

// #[utoipa::path(
//     post,
//     path = "/ui/databases/{databaseName}/schemas/{schemaName}/tables",
//     operation_id = "createTable",
//     tags = ["tables"],
//     params(
//         ("databaseName" = String, description = "Database Name"),
//         ("schemaName" = String, description = "Schema Name")
//     ),
//     request_body = CreateTablePayload,
//     responses(
//         (status = 200, description = "Successful Response"),
//         (status = 400, description = "Bad request", body = ErrorResponse),
//         (status = 422, description = "Unprocessable entity", body = ErrorResponse),
//         (status = 500, description = "Internal server error", body = ErrorResponse)
//     )
// )]
// #[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// pub async fn create_table(
//     DFSessionId(session_id): DFSessionId,
//     State(state): State<AppState>,
//     Path((database_name, schema_name)): Path<(String, String)>,
//     Json(payload): Json<CreateTablePayload>,
// ) -> UIResult<()> {
//     let context = IceBucketQueryContext {
//         database: Some(database_name),
//         schema: Some(schema_name),
//     };
//
//
//
//     let _ = state
//         .execution_svc
//         .query(&session_id, "", context)
//         .await
//         .map_err(|e| UIError::Execution { source: e })?;
//
//     Ok(())
//
// }

#[utoipa::path(
    get,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}/{tableName}/info",
    params(
        ("databaseName" = String, description = "Database Name"),
        ("schemaName" = String, description = "Schema Name"),
        ("tableName" = String, description = "Table Name")
    ),
    operation_id = "getTableInfo",
    tags = ["tables"],
    responses(
        (status = 200, description = "Successful Response", body = TableInfoResponse),
        (status = 404, description = "Table not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
#[allow(clippy::unwrap_used)]
pub async fn get_table_info(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
) -> TablesResult<Json<TableInfoResponse>> {
    let context = IceBucketQueryContext {
        database: Some(database_name.clone()),
        schema: Some(schema_name.clone()),
    };
    let sql_string = format!("SELECT column_name, data_type FROM datafusion.information_schema.columns WHERE table_name = '{table_name}'");
    let (batches, _) = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context.clone())
        .await
        .map_err(|e| TablesAPIError::Get { source: e })?;
    let mut columns: Vec<TableInfoColumn> = vec![];
    for batch in batches {
        let column_name_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let data_type_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // Iterate over each record
        for i in 0..batch.num_rows() {
            columns.push(TableInfoColumn {
                name: column_name_array.value(i).to_string(),
                r#type: data_type_array.value(i).to_string(),
            });
        }
    }
    let sql_string = format!(
        "SELECT COUNT(*) AS total_rows FROM {database_name}.{schema_name}.{}",
        table_name.clone()
    );
    let (batches, _) = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .map_err(|e| TablesAPIError::Get { source: e })?;
    let total_rows = batches.first().map_or(0, |batch| {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .map_or(0, |array| array.value(0))
    });
    Ok(Json(TableInfoResponse {
        data: TableInfo {
            name: table_name,
            columns,
            total_rows,
        },
    }))
}

#[utoipa::path(
    get,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}/{tableName}/preview",
    params(
        ("databaseName" = String, description = "Database Name"),
        ("schemaName" = String, description = "Schema Name"),
        ("tableName" = String, description = "Table Name"),
        ("offset" = Option<u32>, Query, description = "Table preview offset"),
        ("limit" = Option<u16>, Query, description = "Table preview limit")
    ),
    operation_id = "getTablePreviewData",
    tags = ["tables"],
    responses(
        (status = 200, description = "Successful Response", body = TablePreviewDataResponse),
        (status = 404, description = "Table not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
#[allow(clippy::unwrap_used)]
pub async fn get_table_preview_data(
    DFSessionId(session_id): DFSessionId,
    Query(parameters): Query<TablePreviewDataParameters>,
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
) -> TablesResult<Json<TablePreviewDataResponse>> {
    let context = IceBucketQueryContext {
        database: Some(database_name.clone()),
        schema: Some(schema_name.clone()),
    };
    let sql_string = format!(
        "SELECT column_name FROM datafusion.information_schema.columns WHERE table_name = '{}'",
        table_name.clone()
    );
    let (batches, _) = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context.clone())
        .await
        .map_err(|e| TablesAPIError::Get { source: e })?;
    let mut column_names: Vec<String> = vec![];
    for batch in batches {
        let column_name_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for column_name in column_name_array {
            column_names.push(column_name.unwrap().to_string());
        }
    }
    let column_names = column_names
        .iter()
        //UNSUPPORTED TYPES: ListArray, StructArray, Binary (Arrow Cast for Datafusion)
        .map(|column_name| format!("COALESCE(CAST({column_name} AS STRING), 'Unsupported')"))
        .collect::<Vec<_>>();
    let sql_string = format!(
        "SELECT {} FROM {database_name}.{schema_name}.{table_name}",
        column_names.join(", ")
    );
    let sql_string = parameters.offset.map_or(sql_string.clone(), |offset| {
        format!("{sql_string} OFFSET {offset}")
    });
    let sql_string = parameters.limit.map_or(sql_string.clone(), |limit| {
        format!("{sql_string} LIMIT {limit}")
    });
    let (batches, _) = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .map_err(|e| TablesAPIError::Get { source: e })?;
    let mut preview_data_columns: Vec<TablePreviewDataColumn> = vec![];
    for batch in batches {
        for (i, column) in batch.columns().iter().enumerate() {
            let mut preview_data_rows: Vec<TablePreviewDataRow> = vec![];
            for row in column.as_any().downcast_ref::<StringArray>().unwrap() {
                preview_data_rows.push(TablePreviewDataRow {
                    data: row.unwrap().to_string(),
                });
            }
            preview_data_columns.push(TablePreviewDataColumn {
                name: batch.schema().fields[i].name().to_string(),
                rows: preview_data_rows,
            });
        }
    }
    Ok(Json(TablePreviewDataResponse {
        items: preview_data_columns,
    }))
}

// #[utoipa::path(
//     delete,
//     path = "/ui/databases/{databaseName}/schemas/{schemaName}",
//     operation_id = "deleteSchema",
//     tags = ["schemas"],
//     params(
//         ("databaseName" = String, description = "Database Name"),
//         ("schemaName" = String, description = "Schema Name")
//     ),
//     responses(
//         (status = 204, description = "Successful Response"),
//         (status = 404, description = "Schema not found", body = ErrorResponse),
//         (status = 422, description = "Unprocessable entity", body = ErrorResponse),
//     )
// )]
// #[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
// pub async fn delete_schema(
//     State(state): State<AppState>,
//     Query(query): Query<QueryParameters>,
//     Path((database_name, schema_name)): Path<(String, String)>,
// ) -> UIResult<()> {
//     let schema_ident = IceBucketSchemaIdent::new(database_name, schema_name);
//     state
//         .metastore
//         .delete_schema(&schema_ident, query.cascade.unwrap_or_default())
//         .await
//         .map_err(|e| UIError::Metastore { source: e })
// }
