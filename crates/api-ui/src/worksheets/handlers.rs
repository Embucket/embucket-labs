#![allow(clippy::needless_for_each)]
use crate::error::{ErrorResponse, Result};
use crate::state::AppState;
use crate::worksheets::error::{DatetimeSnafu, ListSnafu};
use crate::worksheets::{
    Worksheet, WorksheetCreatePayload, WorksheetCreateResponse, WorksheetResponse,
    WorksheetUpdatePayload, WorksheetsResponse, error as worksheets_error,
};
use crate::{
    OrderDirection, SearchParameters, apply_parameters, downcast_int64_column,
    downcast_string_column,
};
use api_sessions::DFSessionId;
use axum::{
    Json,
    extract::{Path, Query, State},
};
use chrono::{DateTime, Utc};
use core_executor::models::{QueryContext, QueryResult};
use core_history::WorksheetId;
use snafu::ResultExt;
use std::convert::From;
use tracing;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        worksheets,
        worksheet,
        create_worksheet,
        delete_worksheet,
        update_worksheet,
    ),
    components(schemas(
        ErrorResponse,
        WorksheetCreatePayload,
        WorksheetUpdatePayload,
        WorksheetCreateResponse,
        WorksheetResponse,
        WorksheetsResponse,
    )),
    tags(
        (name = "worksheets", description = "Worksheets endpoints"),
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    path = "/ui/worksheets",
    operation_id = "getWorksheets",
    tags = ["worksheets"],
    params(
        ("offset" = Option<usize>, Query, description = "Worksheets offset"),
        ("limit" = Option<usize>, Query, description = "Worksheets limit"),
        ("search" = Option<String>, Query, description = "Worksheets search"),
        ("order_by" = Option<String>, Query, description = "Order by: id (default), name, content, created_at, updated_at"),
        ("order_direction" = Option<OrderDirection>, Query, description = "Order direction: ASC, DESC (default)"),
    ),
    responses(
        (status = 200, description = "Get list of worksheets", body = WorksheetsResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 400, description = "Unknown", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(name = "api_ui::worksheets", level = "info", skip(state), err)]
pub async fn worksheets(
    State(state): State<AppState>,
    DFSessionId(session_id): DFSessionId,
    Query(parameters): Query<SearchParameters>,
) -> Result<Json<WorksheetsResponse>> {
    let context = QueryContext::default();
    let sql_string = "SELECT * FROM slatedb.history.worksheets".to_string();
    let sql_string = apply_parameters(&sql_string, parameters, &["id", "name", "content"]);
    let QueryResult { records, .. } = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .context(ListSnafu)?;
    let mut items = Vec::new();
    for record in records {
        let ids = downcast_int64_column(&record, "id").context(ListSnafu)?;
        let names = downcast_string_column(&record, "name").context(ListSnafu)?;
        let contents = downcast_string_column(&record, "content").context(ListSnafu)?;
        let created_at_timestamps =
            downcast_string_column(&record, "created_at").context(ListSnafu)?;
        let updated_at_timestamps =
            downcast_string_column(&record, "updated_at").context(ListSnafu)?;
        for i in 0..record.num_rows() {
            items.push(Worksheet {
                id: ids.value(i),
                name: names.value(i).to_string(),
                content: contents.value(i).to_string(),
                created_at: created_at_timestamps
                    .value(i)
                    .parse::<DateTime<Utc>>()
                    .context(DatetimeSnafu)?,
                updated_at: updated_at_timestamps
                    .value(i)
                    .parse::<DateTime<Utc>>()
                    .context(DatetimeSnafu)?,
            });
        }
    }

    Ok(Json(WorksheetsResponse { items }))
}

#[utoipa::path(
    post,
    path = "/ui/worksheets",
    operation_id = "createWorksheet",
    tags = ["worksheets"],
    request_body(
        content(
            (
                WorksheetCreatePayload = "application/json", 
                examples (
                    ("with name" = (
                        value = json!(WorksheetCreatePayload {
                            name: "worksheet1".to_string(), 
                            content: "select 1".to_string(),
                        })
                    )),
                    ("empty name" = (
                        value = json!(WorksheetCreatePayload {
                            name: String::new(),
                            content: "select 1".to_string(),
                        })
                    )),
                )
            ),
        )
    ),
    responses(
        (status = 200, description = "Created worksheet", body = WorksheetCreateResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 409, description = "Already Exists", body = ErrorResponse),
        (status = 422, description = "Unprocessable Entity"), // Failed to deserialize payload
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(name = "api_ui::create_worksheet", level = "info", skip(state, payload), err, ret(level = tracing::Level::TRACE))]
pub async fn create_worksheet(
    State(state): State<AppState>,
    Json(payload): Json<WorksheetCreatePayload>,
) -> Result<Json<WorksheetCreateResponse>> {
    let name = if payload.name.is_empty() {
        Utc::now().to_string()
    } else {
        payload.name
    };

    let history_worksheet = core_history::Worksheet::new(name, payload.content);

    let worksheet = state
        .history_store
        .add_worksheet(history_worksheet)
        .await
        .context(worksheets_error::StoreSnafu)
        .context(worksheets_error::CreateSnafu)?
        .into();

    Ok(Json(WorksheetCreateResponse(worksheet)))
}

#[utoipa::path(
    get,
    path = "/ui/worksheets/{worksheetId}",
    operation_id = "getWorksheet",
    tags = ["worksheets"],
    params(
        ("worksheetId" = WorksheetId, Path, description = "Worksheet id")
    ),
    responses(
        (status = 200, description = "Returns worksheet", body = WorksheetResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),        
        (status = 404, description = "Worksheet not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(name = "api_ui::worksheet", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn worksheet(
    State(state): State<AppState>,
    Path(worksheet_id): Path<WorksheetId>,
) -> Result<Json<WorksheetResponse>> {
    let history_worksheet = state
        .history_store
        .get_worksheet(worksheet_id)
        .await
        .context(worksheets_error::StoreSnafu)
        .context(worksheets_error::GetSnafu)?;

    Ok(Json(WorksheetResponse(Worksheet::from(history_worksheet))))
}

#[utoipa::path(
    delete,
    path = "/ui/worksheets/{worksheetId}",
    operation_id = "deleteWorksheet",
    tags = ["worksheets"],
    params(
        ("worksheetId" = WorksheetId, Path, description = "Worksheet id")
    ),
    responses(
        (status = 200, description = "Worksheet deleted"),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 404, description = "Worksheet not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(name = "api_ui::delete_worksheet", level = "info", skip(state), err)]
pub async fn delete_worksheet(
    State(state): State<AppState>,
    Path(worksheet_id): Path<WorksheetId>,
) -> Result<()> {
    state
        .history_store
        .delete_worksheet(worksheet_id)
        .await
        .context(worksheets_error::StoreSnafu)
        .context(worksheets_error::DeleteSnafu)?;

    Ok(())
}

#[utoipa::path(
    patch,
    path = "/ui/worksheets/{worksheetId}",
    operation_id = "updateWorksheet",
    tags = ["worksheets"],
    params(
        ("worksheetId" = WorksheetId, Path, description = "Worksheet id")
    ),
    request_body(
        content(
            (
                WorksheetUpdatePayload = "application/json", 
                examples (
                    ("rename" = (
                        value = json!(WorksheetUpdatePayload {
                            name: Some("new-worksheet".into()),
                            content: None,
                        })
                    )),
                    ("update content" = (
                        value = json!(WorksheetUpdatePayload {
                            name: None,
                            content: Some("SELECT * from test;".into()),
                        })
                    )),
                )
            ),
        )
    ),
    responses(
        (status = 200, description = "Worksheet updated"),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 404, description = "Worksheet not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(
    name = "api_ui::update_worksheet",
    level = "info",
    skip(state, payload),
    err
)]
pub async fn update_worksheet(
    State(state): State<AppState>,
    Path(worksheet_id): Path<WorksheetId>,
    Json(payload): Json<WorksheetUpdatePayload>,
) -> Result<()> {
    if payload.name.is_none() && payload.content.is_none() {
        return worksheets_error::NothingToUpdateSnafu
            .fail()
            .context(worksheets_error::UpdateSnafu)?;
    }

    let mut worksheet = state
        .history_store
        .get_worksheet(worksheet_id)
        .await
        .context(worksheets_error::StoreSnafu)
        .context(worksheets_error::GetSnafu)?;

    if let Some(name) = payload.name {
        worksheet.set_name(name);
    }

    if let Some(content) = payload.content {
        worksheet.set_content(content);
    }

    state
        .history_store
        .update_worksheet(worksheet)
        .await
        .context(worksheets_error::StoreSnafu)
        .context(worksheets_error::UpdateSnafu)?;

    Ok(())
}
