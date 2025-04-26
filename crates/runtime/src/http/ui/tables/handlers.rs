use crate::execution::query::QueryContext;
use crate::http::error::ErrorResponse;
use crate::http::session::DFSessionId;
use crate::http::state::AppState;
use crate::http::ui::tables::error::{
    CreateUploadSnafu, MalformedMultipartFileDataSnafu, MalformedMultipartSnafu, TableError,
    TablesAPIError, TablesResult,
};
use crate::http::ui::tables::models::{
    Table, TableColumn, TableColumnsResponse, TablePreviewDataColumn, TablePreviewDataParameters,
    TablePreviewDataResponse, TablePreviewDataRow, TableStatistics, TableStatisticsResponse,
    TableUploadPayload, TableUploadResponse, TablesParameters, TablesResponse, UploadParameters,
};
use arrow_array::{Array, StringArray};
use axum::extract::Query;
use axum::{
    extract::{Multipart, Path, State},
    Json,
};
use datafusion::arrow::csv::reader::Format;
use embucket_metastore::error::MetastoreError;
use embucket_metastore::{SchemaIdent as MetastoreSchemaIdent, TableIdent as MetastoreTableIdent};
use embucket_utils::scan_iterator::ScanIterator;
use snafu::ResultExt;
use std::time::Instant;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        get_table_statistics,
        get_table_columns,
        get_table_preview_data,
        upload_file,
    ),
    components(
        schemas(
            TableStatisticsResponse,
            TableStatistics,
            TableColumnsResponse,
            TableColumn,
            TablePreviewDataResponse,
            TablePreviewDataColumn,
            TablePreviewDataRow,
            UploadParameters,
            TableUploadPayload,
            TableUploadResponse,
            ErrorResponse,
        )
    ),
    tags(
        (name = "tables", description = "Tables endpoints.")
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}/tables/{tableName}/statistics",
    params(
        ("databaseName" = String, description = "Database Name"),
        ("schemaName" = String, description = "Schema Name"),
        ("tableName" = String, description = "Table Name")
    ),
    operation_id = "getTableStatistics",
    tags = ["tables"],
    responses(
        (status = 200, description = "Successful Response", body = TableStatisticsResponse),
        (status = 404, description = "Table not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
#[allow(clippy::unwrap_used)]
pub async fn get_table_statistics(
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
) -> TablesResult<Json<TableStatisticsResponse>> {
    let ident = MetastoreTableIdent::new(&database_name, &schema_name, &table_name);
    match state.metastore.get_table(&ident).await {
        Ok(Some(rw_object)) => {
            let mut total_bytes = 0;
            let mut total_rows = 0;
            if let Ok(Some(latest_snapshot)) = rw_object.metadata.current_snapshot(None) {
                total_bytes = latest_snapshot
                    .summary()
                    .other
                    .get("total-files-size")
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(0);
                total_rows = latest_snapshot
                    .summary()
                    .other
                    .get("total-records")
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(0);
            }
            Ok(Json(TableStatisticsResponse {
                data: TableStatistics {
                    name: rw_object.ident.table.clone(),
                    total_rows,
                    total_bytes,
                    created_at: rw_object.created_at,
                    updated_at: rw_object.updated_at,
                },
            }))
        }
        Ok(None) => Err(TablesAPIError::GetMetastore {
            source: MetastoreError::TableNotFound {
                table: database_name,
                schema: schema_name,
                db: table_name,
            },
        }),
        Err(e) => Err(TablesAPIError::GetMetastore { source: e }),
    }
}
#[utoipa::path(
    get,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}/tables/{tableName}/columns",
    params(
        ("databaseName" = String, description = "Database Name"),
        ("schemaName" = String, description = "Schema Name"),
        ("tableName" = String, description = "Table Name")
    ),
    operation_id = "getTableColumns",
    tags = ["tables"],
    responses(
        (status = 200, description = "Successful Response", body = TableColumnsResponse),
        (status = 404, description = "Table not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
#[allow(clippy::unwrap_used)]
pub async fn get_table_columns(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
) -> TablesResult<Json<TableColumnsResponse>> {
    let context = QueryContext {
        database: Some(database_name.clone()),
        schema: Some(schema_name.clone()),
    };
    let sql_string = format!("SELECT * FROM {database_name}.{schema_name}.{table_name} LIMIT 0");
    let (_, column_infos) = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .map_err(|e| TablesAPIError::GetExecution { source: e })?;
    let items: Vec<TableColumn> = column_infos
        .iter()
        .map(|column_info| TableColumn {
            name: column_info.name.clone(),
            r#type: column_info.r#type.clone(),
            description: String::new(),
            nullable: if column_info.nullable {
                "Y".to_string()
            } else {
                "N".to_string()
            },
            default: if column_info.nullable {
                "NULL".to_string()
            } else {
                String::new()
            },
        })
        .collect();
    Ok(Json(TableColumnsResponse { items }))
}
#[utoipa::path(
    get,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}/tables/{tableName}/rows",
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
    let context = QueryContext {
        database: Some(database_name.clone()),
        schema: Some(schema_name.clone()),
    };
    let ident = MetastoreTableIdent::new(&database_name, &schema_name, &table_name);
    let column_names = match state.metastore.get_table(&ident).await {
        Ok(Some(rw_object)) => {
            if let Ok(schema) = rw_object.metadata.current_schema(None) {
                let items: Vec<String> = schema.iter().map(|field| field.name.clone()).collect();
                Ok(items)
            } else {
                Ok(vec![])
            }
        }
        Ok(None) => Err(TablesAPIError::GetMetastore {
            source: MetastoreError::TableNotFound {
                table: database_name.clone(),
                schema: schema_name.clone(),
                db: table_name.clone(),
            },
        }),
        Err(e) => Err(TablesAPIError::GetMetastore { source: e }),
    }?;
    let column_names = column_names
        .iter()
        //UNSUPPORTED TYPES: ListArray, StructArray, Binary (Arrow Cast for Datafusion)
        .map(|column_name| {
            format!("COALESCE(CAST({column_name} AS STRING), 'Unsupported') AS {column_name}")
        })
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
        .map_err(|e| TablesAPIError::GetExecution { source: e })?;
    let mut preview_data_columns: Vec<TablePreviewDataColumn> = vec![];
    for batch in &batches {
        for (i, column) in batch.columns().iter().enumerate() {
            let preview_data_rows: Vec<TablePreviewDataRow> = column
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|row| TablePreviewDataRow {
                    data: row.unwrap().to_string(),
                })
                .collect();
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

#[utoipa::path(
    post,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}/tables/{tableName}/rows",
    operation_id = "uploadFile",
    tags = ["tables"],
    params(
        ("databaseName" = String, description = "Database Name"),
        ("schemaName" = String, description = "Schema Name"),
        ("tableName" = String, description = "Table Name"),
        ("header" = Option<bool>, Query, example = json!(true), description = "Has header"),
        ("delimiter" = Option<u8>, Query, description = "an optional column delimiter, defaults to comma `','`"),
        ("escape" = Option<u8>, Query, description = "an escape character"),
        ("quote" = Option<u8>, Query, description = "a custom quote character, defaults to double quote `'\"'`"),
        ("terminator" = Option<u8>, Query, description = "a custom terminator character, defaults to CRLF"),
        ("comment" = Option<u8>, Query, description = "a comment character"),
    ),
    request_body(
        content = TableUploadPayload,
        content_type = "multipart/form-data",
        description = "Upload data to the table in multipart/form-data format"
    ),
    responses(
        (status = 200, description = "Successful Response", body = TableUploadResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        // 409, when schema provided but table already exists
        (status = 409, description = "Already exists", body = ErrorResponse),
        (status = 422, description = "Unprocessable content", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state, multipart), err, ret(level = tracing::Level::TRACE))]
pub async fn upload_file(
    DFSessionId(session_id): DFSessionId,
    Query(parameters): Query<UploadParameters>,
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
    mut multipart: Multipart,
) -> TablesResult<Json<TableUploadResponse>> {
    let mut uploaded = false;
    let mut rows_loaded: usize = 0;
    let start = Instant::now();
    let parameters: Format = parameters.into();
    while let Some(field) = multipart
        .next_field()
        .await
        .context(MalformedMultipartSnafu)
        .context(CreateUploadSnafu)?
    {
        if let Some(file_name) = field.file_name() {
            let file_name = String::from(file_name);
            let data = field
                .bytes()
                .await
                .context(MalformedMultipartFileDataSnafu)
                .context(CreateUploadSnafu)?;

            rows_loaded += state
                .execution_svc
                .upload_data_to_table(
                    &session_id,
                    &MetastoreTableIdent {
                        table: table_name.clone(),
                        schema: schema_name.clone(),
                        database: database_name.clone(),
                    },
                    data,
                    file_name.as_str(),
                    parameters.clone(),
                )
                .await
                .map_err(|e| TablesAPIError::CreateUpload {
                    source: TableError::Execution { source: e },
                })?;
            uploaded = true;
        }
    }
    let duration = start.elapsed();
    if uploaded {
        Ok(Json(TableUploadResponse {
            count: rows_loaded,
            duration_ms: duration.as_millis(),
        }))
    } else {
        Err(TablesAPIError::CreateUpload {
            source: TableError::FileField,
        })
    }
}

#[utoipa::path(
    get,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}/tables/{schemaName}/tables",
    params(
        ("databaseName" = String, description = "Database Name"),
        ("schemaName" = String, description = "Schema Name"),
        ("cursor" = Option<String>, Query, description = "Tables cursor"),
        ("limit" = Option<usize>, Query, description = "Tables limit"),
        ("search" = Option<String>, Query, description = "Tables search (start with)"),
    ),
    operation_id = "getTables",
    tags = ["tables"],
    responses(
        (status = 200, description = "Successful Response", body = TablesResponse),
        (status = 404, description = "Table not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
#[allow(clippy::unwrap_used)]
pub async fn get_tables(
    Query(parameters): Query<TablesParameters>,
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> TablesResult<Json<TablesResponse>> {
    let ident = MetastoreSchemaIdent::new(database_name, schema_name);
    state
        .metastore
        .iter_tables(&ident)
        .cursor(parameters.cursor.clone())
        .limit(parameters.limit)
        .token(parameters.search)
        .collect()
        .await
        .map_err(|e| TablesAPIError::GetMetastore {
            source: MetastoreError::UtilSlateDB { source: e },
        })
        .map(|rw_tables| {
            let next_cursor = rw_tables
                .iter()
                .last()
                .map_or(String::new(), |rw_table| rw_table.ident.table.clone());

            let items = rw_tables
                .into_iter()
                .map(|rw_table| {
                    let mut total_bytes = 0;
                    let mut total_rows = 0;
                    if let Ok(Some(latest_snapshot)) = rw_table.metadata.current_snapshot(None) {
                        total_bytes = latest_snapshot
                            .summary()
                            .other
                            .get("total-files-size")
                            .and_then(|value| value.parse::<i64>().ok())
                            .unwrap_or(0);
                        total_rows = latest_snapshot
                            .summary()
                            .other
                            .get("total-records")
                            .and_then(|value| value.parse::<i64>().ok())
                            .unwrap_or(0);
                    }
                    Table {
                        name: rw_table.ident.table.clone(),
                        r#type: "TABLE".to_string(),
                        owner: String::new(),
                        total_rows,
                        total_bytes,
                        created_at: rw_table.created_at,
                        updated_at: rw_table.updated_at,
                    }
                })
                .collect();
            Ok(Json(TablesResponse {
                items,
                current_cursor: parameters.cursor,
                next_cursor,
            }))
        })?
}
