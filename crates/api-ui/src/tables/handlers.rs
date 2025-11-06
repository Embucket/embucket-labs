#![allow(clippy::needless_for_each)]
use crate::error::{ErrorResponse, Result};
use crate::state::AppState;
use crate::tables::error::{
    self as tables_errors, ExecutionSnafu, MalformedMultipartFileDataSnafu,
    MalformedMultipartSnafu, UploadFileSnafu,
};
use crate::tables::models::{
    Table, TableColumn, TableColumnsResponse, TablePreviewDataColumn, TablePreviewDataParameters,
    TablePreviewDataResponse, TablePreviewDataRow, TableStatistics, TableStatisticsResponse,
    TableUploadPayload, TableUploadResponse, TablesResponse, UploadParameters,
};
use crate::{
    OrderDirection, SearchParameters, apply_parameters, downcast_int64_column,
    downcast_string_column,
};
use api_sessions::DFSessionId;
use axum::extract::Query;
use axum::{
    Json,
    extract::{Multipart, Path, State},
};
use core_executor::models::{QueryContext, QueryResult};
use core_metastore::TableIdent as MetastoreTableIdent;
use core_metastore::error as metastore_error;
use datafusion::arrow::csv::reader::Format;
use datafusion::arrow::util::display::array_value_to_string;
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
        get_tables,
    ),
    components(
        schemas(
            TableStatisticsResponse,
            TableStatistics,
            TableColumnsResponse,
            TableColumn,
            TablePreviewDataResponse,
            TablePreviewDataParameters,
            TablePreviewDataColumn,
            TablePreviewDataRow,
            UploadParameters,
            TableUploadPayload,
            TableUploadResponse,
            TablesResponse,
            ErrorResponse,
            OrderDirection,
            Table,
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
        ("databaseName" = String, Path, description = "Database Name"),
        ("schemaName" = String, Path, description = "Schema Name"),
        ("tableName" = String, Path, description = "Table Name")
    ),
    operation_id = "getTableStatistics",
    tags = ["tables"],
    responses(
        (status = 200, description = "Successful Response", body = TableStatisticsResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 404, description = "Table not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(name = "api_ui::get_table_statistics", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
#[allow(clippy::unwrap_used)]
pub async fn get_table_statistics(
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
) -> Result<Json<TableStatisticsResponse>> {
    let ident = MetastoreTableIdent::new(&database_name, &schema_name, &table_name);
    let table = match state.metastore.get_table(&ident).await {
        Ok(Some(rw_object)) => {
            let total_bytes =
                if let Ok(Some(latest_snapshot)) = rw_object.metadata.current_snapshot(None) {
                    latest_snapshot
                        .summary()
                        .other
                        .get("total-files-size")
                        .and_then(|value| value.parse::<i64>().ok())
                        .unwrap_or(0)
                } else {
                    0
                };
            let total_rows =
                if let Ok(Some(latest_snapshot)) = rw_object.metadata.current_snapshot(None) {
                    latest_snapshot
                        .summary()
                        .other
                        .get("total-records")
                        .and_then(|value| value.parse::<i64>().ok())
                        .unwrap_or(0)
                } else {
                    0
                };
            Ok(Json(TableStatisticsResponse(TableStatistics {
                name: rw_object.ident.table.clone(),
                total_rows,
                total_bytes,
                created_at: rw_object.created_at,
                updated_at: rw_object.updated_at,
            })))
        }
        Ok(None) => Err(metastore_error::TableNotFoundSnafu {
            table: database_name,
            schema: schema_name,
            db: table_name,
        }
        .build()),
        Err(source) => Err(source),
    }
    .context(tables_errors::MetastoreSnafu)
    .context(tables_errors::GetTableStatisticsSnafu)?;

    Ok(table)
}
#[utoipa::path(
    get,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}/tables/{tableName}/columns",
    params(
        ("databaseName" = String, Path, description = "Database Name"),
        ("schemaName" = String, Path, description = "Schema Name"),
        ("tableName" = String, Path, description = "Table Name")
    ),
    operation_id = "getTableColumns",
    tags = ["tables"],
    responses(
        (status = 200, description = "Successful Response", body = TableColumnsResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 404, description = "Table not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(name = "api_ui::get_table_columns", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
#[allow(clippy::unwrap_used)]
pub async fn get_table_columns(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Path((database, schema, table)): Path<(String, String, String)>,
) -> Result<Json<TableColumnsResponse>> {
    let context = QueryContext::new(Some(database.clone()), Some(schema.clone()), None);
    let sql_string = format!("SELECT * FROM `{database}`.`{schema}`.`{table}` LIMIT 0");
    let columns_info = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .context(ExecutionSnafu)
        .context(tables_errors::GetTableColumnsSnafu)?
        .column_info();
    let items: Vec<TableColumn> = columns_info
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
        ("databaseName" = String, Path, description = "Database Name"),
        ("schemaName" = String, Path, description = "Schema Name"),
        ("tableName" = String, Path, description = "Table Name"),
        ("offset" = Option<u32>, Query, description = "Table preview offset"),
        ("limit" = Option<u16>, Query, description = "Table preview limit")
    ),
    operation_id = "getTablePreviewData",
    tags = ["tables"],
    responses(
        (status = 200, description = "Successful Response", body = TablePreviewDataResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 404, description = "Table not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(name = "api_ui::get_table_preview_data", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
#[allow(clippy::unwrap_used)]
pub async fn get_table_preview_data(
    DFSessionId(session_id): DFSessionId,
    Query(parameters): Query<TablePreviewDataParameters>,
    State(state): State<AppState>,
    Path((database, schema, table)): Path<(String, String, String)>,
) -> Result<Json<TablePreviewDataResponse>> {
    let context = QueryContext::new(Some(database.clone()), Some(schema.clone()), None);
    let sql_string = format!("SELECT * FROM {database}.{schema}.{table}");
    let sql_string = parameters.offset.map_or_else(
        || sql_string.clone(),
        |offset| format!("{sql_string} OFFSET {offset}"),
    );
    let sql_string = parameters.limit.map_or_else(
        || sql_string.clone(),
        |limit| format!("{sql_string} LIMIT {limit}"),
    );
    let QueryResult {
        records: batches, ..
    } = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .context(ExecutionSnafu)
        .context(tables_errors::GetTablePreviewDataSnafu)?;

    let mut preview_data_columns = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        let num_rows = batch.num_rows();

        for (i, column) in batch.columns().iter().enumerate() {
            let array = column.as_ref();

            let preview_data_rows: Vec<TablePreviewDataRow> = (0..num_rows)
                .map(|row_index| {
                    let data = array_value_to_string(array, row_index)
                        .unwrap_or_else(|_| "ERROR".to_string());
                    TablePreviewDataRow { data }
                })
                .collect();

            preview_data_columns.push(TablePreviewDataColumn {
                name: schema.field(i).name().to_string(),
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
        ("databaseName" = String, Path, description = "Database Name"),
        ("schemaName" = String, Path, description = "Schema Name"),
        ("tableName" = String, Path, description = "Table Name"),
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
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        // 409, when schema provided but table already exists
        (status = 409, description = "Already exists", body = ErrorResponse),
        (status = 422, description = "Unprocessable content", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(name = "api_ui::upload_file", level = "info", skip(state, multipart), err, ret(level = tracing::Level::TRACE))]
pub async fn upload_file(
    DFSessionId(session_id): DFSessionId,
    Query(parameters): Query<UploadParameters>,
    State(state): State<AppState>,
    Path((database_name, schema_name, table_name)): Path<(String, String, String)>,
    mut multipart: Multipart,
) -> Result<Json<TableUploadResponse>> {
    let mut uploaded = false;
    let mut rows_loaded: usize = 0;
    let start = Instant::now();
    let parameters: Format = parameters.into();
    while let Some(field) = multipart
        .next_field()
        .await
        .context(MalformedMultipartSnafu)
        .context(UploadFileSnafu)?
    {
        if let Some(file_name) = field.file_name() {
            let file_name = String::from(file_name);
            let data = field
                .bytes()
                .await
                .context(MalformedMultipartFileDataSnafu)
                .context(UploadFileSnafu)?;

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
                .context(ExecutionSnafu)
                .context(UploadFileSnafu)?;
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
        tables_errors::FileFieldSnafu
            .fail()
            .context(tables_errors::UploadFileSnafu)?
    }
}

#[utoipa::path(
    get,
    path = "/ui/databases/{databaseName}/schemas/{schemaName}/tables",
    params(
        ("databaseName" = String, Path, description = "Database Name"),
        ("schemaName" = String, Path, description = "Schema Name"),
        ("offset" = Option<usize>, Query, description = "Tables offset"),
        ("limit" = Option<usize>, Query, description = "Tables limit"),
        ("search" = Option<String>, Query, description = "Tables search"),
        ("orderBy" = Option<String>, Query, description = "Order by: table_name, schema_name, database_name, volume_name, table_type, table_format, owner, created_at (default), updated_at"),
        ("orderDirection" = Option<OrderDirection>, Query, description = "Order direction: ASC, DESC (default)"),
    ),
    operation_id = "getTables",
    tags = ["tables"],
    responses(
        (status = 200, description = "Successful Response", body = TablesResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 404, description = "Table not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(name = "api_ui::get_tables", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
#[allow(clippy::unwrap_used)]
pub async fn get_tables(
    DFSessionId(session_id): DFSessionId,
    Query(parameters): Query<SearchParameters>,
    State(state): State<AppState>,
    Path((database_name, schema_name)): Path<(String, String)>,
) -> Result<Json<TablesResponse>> {
    let context = QueryContext::new(Some(database_name.clone()), None, None);
    let sql_string = format!(
        "SELECT * FROM sqlite.meta.tables WHERE schema_name = '{}' AND database_name = '{}'",
        schema_name.clone(),
        database_name.clone()
    );
    let sql_string = apply_parameters(
        &sql_string,
        parameters,
        &[
            "table_name",
            "volume_name",
            "table_type",
            "table_format",
            "owner",
        ],
        "created_at",
        OrderDirection::DESC,
    );
    let QueryResult { records, .. } = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .context(ExecutionSnafu)
        .context(tables_errors::GetTablesSnafu)?;
    let mut items = Vec::new();
    for record in records {
        let table_names = downcast_string_column(&record, "table_name")
            .context(ExecutionSnafu)
            .context(tables_errors::GetTablesSnafu)?;
        let schema_names = downcast_string_column(&record, "schema_name")
            .context(ExecutionSnafu)
            .context(tables_errors::GetTablesSnafu)?;
        let database_names = downcast_string_column(&record, "database_name")
            .context(ExecutionSnafu)
            .context(tables_errors::GetTablesSnafu)?;
        let volume_names = downcast_string_column(&record, "volume_name")
            .context(ExecutionSnafu)
            .context(tables_errors::GetTablesSnafu)?;
        let owners = downcast_string_column(&record, "owner")
            .context(ExecutionSnafu)
            .context(tables_errors::GetTablesSnafu)?;
        let table_types = downcast_string_column(&record, "table_type")
            .context(ExecutionSnafu)
            .context(tables_errors::GetTablesSnafu)?;
        let table_format_values = downcast_string_column(&record, "table_format")
            .context(ExecutionSnafu)
            .context(tables_errors::GetTablesSnafu)?;
        let total_bytes_values = downcast_int64_column(&record, "total_bytes")
            .context(ExecutionSnafu)
            .context(tables_errors::GetTablesSnafu)?;
        let total_rows_values = downcast_int64_column(&record, "total_rows")
            .context(ExecutionSnafu)
            .context(tables_errors::GetTablesSnafu)?;
        let created_at_timestamps = downcast_string_column(&record, "created_at")
            .context(ExecutionSnafu)
            .context(tables_errors::GetTablesSnafu)?;
        let updated_at_timestamps = downcast_string_column(&record, "updated_at")
            .context(ExecutionSnafu)
            .context(tables_errors::GetTablesSnafu)?;
        for i in 0..record.num_rows() {
            items.push(Table {
                name: table_names.value(i).to_string(),
                schema_name: schema_names.value(i).to_string(),
                database_name: database_names.value(i).to_string(),
                volume_name: volume_names.value(i).to_string(),
                owner: owners.value(i).to_string(),
                table_format: table_format_values.value(i).to_string(),
                r#type: table_types.value(i).to_string(),
                total_bytes: total_bytes_values.value(i),
                total_rows: total_rows_values.value(i),
                created_at: created_at_timestamps.value(i).to_string(),
                updated_at: updated_at_timestamps.value(i).to_string(),
            });
        }
    }
    Ok(Json(TablesResponse { items }))
}
