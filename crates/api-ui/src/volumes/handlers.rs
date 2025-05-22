use crate::state::AppState;
use crate::{
    OrderDirection, SearchParameters, apply_parameters, downcast_string_column,
    error::ErrorResponse,
    volumes::error::{VolumesAPIError, VolumesResult},
    volumes::models::{
        FileVolume, S3TablesVolume, S3Volume, Volume, VolumeCreatePayload, VolumeCreateResponse,
        VolumePayload, VolumeResponse, VolumeType, VolumeUpdatePayload, VolumeUpdateResponse,
        VolumesResponse,
    },
};
use api_sessions::DFSessionId;
use axum::{
    Json,
    extract::{Path, Query, State},
};
use core_executor::models::QueryResultData;
use core_executor::query::QueryContext;
use core_metastore::error::MetastoreError;
use core_metastore::models::{AwsAccessKeyCredentials, AwsCredentials, Volume as MetastoreVolume};
use utoipa::OpenApi;
use validator::Validate;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_volume,
        get_volume,
        delete_volume,
        list_volumes,
        // update_volume,
    ),
    components(
        schemas(
            VolumeCreatePayload,
            VolumeCreateResponse,
            VolumePayload,
            Volume,
            VolumeType,
            S3Volume,
            S3TablesVolume,
            FileVolume,
            AwsCredentials,
            AwsAccessKeyCredentials,
            VolumeResponse,
            VolumesResponse,
            ErrorResponse,
            OrderDirection,
        )
    ),
    tags(
        (name = "volumes", description = "Volumes endpoints")
    )
)]
pub struct ApiDoc;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryParameters {
    #[serde(default)]
    pub cascade: Option<bool>,
}

#[utoipa::path(
    post,
    operation_id = "createVolume",
    tags = ["volumes"],
    path = "/ui/volumes",
    request_body = VolumeCreatePayload,
    responses(
        (status = 200, description = "Successful Response", body = VolumeCreateResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_volume(
    State(state): State<AppState>,
    Json(volume): Json<VolumeCreatePayload>,
) -> VolumesResult<Json<VolumeCreateResponse>> {
    let embucket_volume: MetastoreVolume = volume.data.into();
    embucket_volume
        .validate()
        .map_err(|e| VolumesAPIError::Create {
            source: MetastoreError::Validation { source: e },
        })?;
    state
        .metastore
        .create_volume(&embucket_volume.ident.clone(), embucket_volume)
        .await
        .map_err(|e| VolumesAPIError::Create { source: e })
        .map(|o| Json(VolumeCreateResponse { data: o.into() }))
}

#[utoipa::path(
    get,
    operation_id = "getVolume",
    tags = ["volumes"],
    path = "/ui/volumes/{volumeName}",
    params(
        ("volumeName" = String, Path, description = "Volume name")
    ),
    responses(
        (status = 200, description = "Successful Response", body = VolumeResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
) -> VolumesResult<Json<VolumeResponse>> {
    match state.metastore.get_volume(&volume_name).await {
        Ok(Some(volume)) => Ok(Json(VolumeResponse {
            data: volume.into(),
        })),
        Ok(None) => Err(VolumesAPIError::Get {
            source: MetastoreError::VolumeNotFound {
                volume: volume_name.clone(),
            },
        }),
        Err(e) => Err(VolumesAPIError::Get { source: e }),
    }
}

#[utoipa::path(
    delete,
    operation_id = "deleteVolume",
    tags = ["volumes"],
    path = "/ui/volumes/{volumeName}",
    params(
        ("volumeName" = String, Path, description = "Volume name")
    ),
    responses(
        (status = 200, description = "Successful Response"),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_volume(
    State(state): State<AppState>,
    Query(query): Query<QueryParameters>,
    Path(volume_name): Path<String>,
) -> VolumesResult<()> {
    state
        .metastore
        .delete_volume(&volume_name, query.cascade.unwrap_or_default())
        .await
        .map_err(|e| VolumesAPIError::Delete { source: e })
}

#[utoipa::path(
    put,
    operation_id = "updateVolume",
    tags = ["volumes"],
    path = "/ui/volumes/{volumeName}",
    params(
        ("volumeName" = String, Path, description = "Volume name")
    ),
    request_body = VolumeUpdatePayload,
    responses(
        (status = 200, description = "Successful Response", body = VolumeUpdateResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 404, description = "Not found", body = ErrorResponse),
        (status = 422, description = "Unprocessable entity", body = ErrorResponse),
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
    Json(volume): Json<VolumeUpdatePayload>,
) -> VolumesResult<Json<VolumeUpdateResponse>> {
    let volume: MetastoreVolume = volume.data.into();
    volume.validate().map_err(|e| VolumesAPIError::Update {
        source: MetastoreError::Validation { source: e },
    })?;
    state
        .metastore
        .update_volume(&volume_name, volume)
        .await
        .map_err(|e| VolumesAPIError::Update { source: e })
        .map(|o| Json(VolumeUpdateResponse { data: o.into() }))
}

#[utoipa::path(
    get,
    operation_id = "getVolumes",
    params(
        ("offset" = Option<usize>, Query, description = "Volumes offset"),
        ("limit" = Option<usize>, Query, description = "Volumes limit"),
        ("search" = Option<String>, Query, description = "Volumes search"),
        ("order_by" = Option<String>, Query, description = "Order by: volume_name (default), volume_type, created_at, updated_at"),
        ("order_direction" = Option<OrderDirection>, Query, description = "Order direction: ASC, DESC (default)"),
    ),
    tags = ["volumes"],
    path = "/ui/volumes",
    responses(
        (status = 200, body = VolumesResponse),
        (status = 401,
         description = "Unauthorized",
         headers(
            ("WWW-Authenticate" = String, description = "Bearer authentication scheme with error details")
         ),
         body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_volumes(
    DFSessionId(session_id): DFSessionId,
    Query(parameters): Query<SearchParameters>,
    State(state): State<AppState>,
) -> VolumesResult<Json<VolumesResponse>> {
    let context = QueryContext::default();
    let sql_string = "SELECT * FROM slatedb.public.volumes".to_string();
    let sql_string = apply_parameters(&sql_string, parameters, &["volume_name", "volume_type"]);
    let QueryResultData { records, .. } = state
        .execution_svc
        .query(&session_id, sql_string.as_str(), context)
        .await
        .map_err(|e| VolumesAPIError::List { source: e })?;
    let mut items = Vec::new();
    for record in records {
        let volume_names = downcast_string_column(&record, "volume_name")
            .map_err(|e| VolumesAPIError::List { source: e })?;
        let volume_types = downcast_string_column(&record, "volume_type")
            .map_err(|e| VolumesAPIError::List { source: e })?;
        let created_at_timestamps = downcast_string_column(&record, "created_at")
            .map_err(|e| VolumesAPIError::List { source: e })?;
        let updated_at_timestamps = downcast_string_column(&record, "updated_at")
            .map_err(|e| VolumesAPIError::List { source: e })?;
        for i in 0..record.num_rows() {
            items.push(Volume {
                name: volume_names.value(i).to_string(),
                r#type: volume_types.value(i).to_string(),
                created_at: created_at_timestamps.value(i).to_string(),
                updated_at: updated_at_timestamps.value(i).to_string(),
            });
        }
    }
    Ok(Json(VolumesResponse { items }))
}
