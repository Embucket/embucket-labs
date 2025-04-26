use crate::http::error::ErrorResponse;
use crate::http::state::AppState;
use crate::http::ui::worksheets::{
    error::{WorksheetUpdateError, WorksheetsAPIError, WorksheetsResult},
    Worksheet, WorksheetCreatePayload, WorksheetCreateResponse, WorksheetResponse,
    WorksheetUpdatePayload, WorksheetsResponse,
};
use axum::{
    extract::{Path, State},
    Json,
};
use chrono::Utc;
use embucket_history::WorksheetId;
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
    responses(
        (status = 200, description = "Get list of worksheets", body = WorksheetsResponse),
        (status = 400, description = "Unknown", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn worksheets(
    State(state): State<AppState>,
) -> WorksheetsResult<Json<WorksheetsResponse>> {
    let history_worksheets = state
        .history
        .get_worksheets()
        .await
        .map_err(|e| WorksheetsAPIError::List { source: e })?;

    let items = history_worksheets
        .into_iter()
        .map(Worksheet::from)
        .collect::<Vec<Worksheet>>();

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
        (status = 409, description = "Already Exists", body = ErrorResponse),
        (status = 422, description = "Unprocessable Entity"), // Failed to deserialize payload
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_worksheet(
    State(state): State<AppState>,
    Json(payload): Json<WorksheetCreatePayload>,
) -> WorksheetsResult<Json<WorksheetCreateResponse>> {
    let name = if payload.name.is_empty() {
        Utc::now().to_string()
    } else {
        payload.name
    };

    let history_worksheet = embucket_history::Worksheet::new(name, payload.content);

    let worksheet = state
        .history
        .add_worksheet(history_worksheet)
        .await
        .map_err(|e| WorksheetsAPIError::Create { source: e })?
        .into();

    Ok(Json(WorksheetCreateResponse { data: worksheet }))
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
        (status = 400, description = "Bad request", body = ErrorResponse),        
        (status = 404, description = "Worksheet not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn worksheet(
    State(state): State<AppState>,
    Path(worksheet_id): Path<WorksheetId>,
) -> WorksheetsResult<Json<WorksheetResponse>> {
    let history_worksheet = state
        .history
        .get_worksheet(worksheet_id)
        .await
        .map_err(|e| WorksheetsAPIError::Get { source: e })?;

    Ok(Json(WorksheetResponse {
        data: Worksheet::from(history_worksheet),
    }))
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
        (status = 404, description = "Worksheet not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_worksheet(
    State(state): State<AppState>,
    Path(worksheet_id): Path<WorksheetId>,
) -> WorksheetsResult<()> {
    //
    // TODO: Decide what to do with queries records related to deleting worksheet, delete it too?
    //
    state
        .history
        .delete_worksheet(worksheet_id)
        .await
        .map_err(|e| WorksheetsAPIError::Delete { source: e })
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
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 404, description = "Worksheet not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_worksheet(
    State(state): State<AppState>,
    Path(worksheet_id): Path<WorksheetId>,
    Json(payload): Json<WorksheetUpdatePayload>,
) -> WorksheetsResult<()> {
    if payload.name.is_none() && payload.content.is_none() {
        return Err(WorksheetsAPIError::Update {
            source: WorksheetUpdateError::NothingToUpdate,
        });
    }

    let mut worksheet = state
        .history
        .get_worksheet(worksheet_id)
        .await
        .map_err(|e| WorksheetsAPIError::Update {
            source: WorksheetUpdateError::Store { source: e },
        })?;

    if let Some(name) = payload.name {
        worksheet.set_name(name);
    }

    if let Some(content) = payload.content {
        worksheet.set_content(content);
    }

    state
        .history
        .update_worksheet(worksheet)
        .await
        .map_err(|e| WorksheetsAPIError::Update {
            source: WorksheetUpdateError::Store { source: e },
        })
}
