#![allow(clippy::needless_for_each)]
use crate::state::AppState;
use crate::volumes::error::VolumeNotFoundSnafu;
use crate::{
    OrderDirection, Result, SearchParameters,
    error::ErrorResponse,
    volumes::error::{CreateQuerySnafu, CreateSnafu, DeleteSnafu, GetSnafu, ListSnafu},
    volumes::models::{
        AwsAccessKeyCredentials, AwsCredentials, FileVolume, S3TablesVolume, S3Volume, Volume,
        VolumeCreatePayload, VolumeCreateResponse, VolumeResponse, VolumeType, VolumesResponse,
    },
};
use api_sessions::DFSessionId;
use axum::{
    Json,
    extract::{Path, Query, State},
};
use core_executor::models::QueryContext;
use core_metastore::error::{ValidationSnafu, VolumeMissingCredentialsSnafu};
use core_metastore::models::{
    AwsCredentials as MetastoreAwsCredentials, Volume as MetastoreVolume,
    VolumeType as MetastoreVolumeType,
};
use snafu::{OptionExt, ResultExt};
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
#[tracing::instrument(name = "api_ui::create_volume", level = "info", skip(state, volume), err, ret(level = tracing::Level::TRACE))]
pub async fn create_volume(
    DFSessionId(session_id): DFSessionId,
    State(state): State<AppState>,
    Json(volume): Json<VolumeCreatePayload>,
) -> Result<Json<VolumeCreateResponse>> {
    let embucket_volume = MetastoreVolume::new(volume.name.clone(), volume.volume.into());
    embucket_volume
        .validate()
        .context(ValidationSnafu)
        .context(CreateSnafu)?;

    let ident = embucket_volume.ident;
    let vol_params = match &embucket_volume.volume {
        MetastoreVolumeType::File(vol) => format!(
            "STORAGE_PROVIDER = 'FILE' STORAGE_BASE_URL = '{}'",
            vol.path
        ),
        MetastoreVolumeType::Memory => "STORAGE_PROVIDER = 'MEMORY'".to_string(),
        MetastoreVolumeType::S3(vol) => {
            let region = vol.region.clone().unwrap_or_default();
            let credentials_str = match &vol.credentials {
                Some(MetastoreAwsCredentials::AccessKey(creds)) => format!(
                    " CREDENTIALS=(AWS_KEY_ID='{}' AWS_SECRET_KEY='{}' REGION='{region}')",
                    creds.aws_access_key_id, creds.aws_secret_access_key,
                ),
                _ => return VolumeMissingCredentialsSnafu.fail().context(CreateSnafu)?,
            };
            let base_url = vol.bucket.clone().unwrap_or_default();
            let endpoint_str = vol
                .endpoint
                .as_ref()
                .map(|e| format!(" STORAGE_ENDPOINT = '{e}'"))
                .unwrap_or_default();
            format!(
                "STORAGE_PROVIDER = 'S3' STORAGE_BASE_URL = '{base_url}'{endpoint_str}{credentials_str}",
            )
        }
        MetastoreVolumeType::S3Tables(vol) => {
            let credentials_str = match &vol.credentials {
                MetastoreAwsCredentials::AccessKey(creds) => format!(
                    " CREDENTIALS=(AWS_KEY_ID='{}' AWS_SECRET_KEY='{}')",
                    creds.aws_access_key_id, creds.aws_secret_access_key
                ),
                MetastoreAwsCredentials::Token(_) => {
                    return VolumeMissingCredentialsSnafu.fail().context(CreateSnafu)?;
                }
            };
            let endpoint_str = vol
                .endpoint
                .as_ref()
                .map(|e| format!(" STORAGE_ENDPOINT = '{e}'"))
                .unwrap_or_default();
            format!(
                "STORAGE_PROVIDER = 'S3TABLES'{endpoint_str} STORAGE_AWS_ACCESS_POINT_ARN = '{}'{}",
                vol.arn, credentials_str
            )
        }
    };
    state
        .execution_svc
        .query(
            &session_id,
            &format!(
                "CREATE EXTERNAL VOLUME '{ident}' STORAGE_LOCATIONS = ((NAME = '{ident}' {vol_params}))",
            ),
            QueryContext::default(),
        )
        .await
        .context(CreateQuerySnafu)?;

    let volume = state
        .metastore
        .get_volume(&ident)
        .await
        .context(GetSnafu)?
        .context(VolumeNotFoundSnafu { volume: ident })?;

    Ok(Json(VolumeCreateResponse(
        Volume::try_from(volume).context(CreateSnafu)?,
    )))
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
#[tracing::instrument(name = "api_ui::get_volume", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
) -> Result<Json<VolumeResponse>> {
    let volume = state
        .metastore
        .get_volume(&volume_name)
        .await
        .context(GetSnafu)?
        .context(VolumeNotFoundSnafu {
            volume: volume_name.clone(),
        })?;

    Ok(Json(VolumeResponse(
        Volume::try_from(volume).context(GetSnafu)?,
    )))
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
#[tracing::instrument(name = "api_ui::delete_volume", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_volume(
    State(state): State<AppState>,
    Query(query): Query<QueryParameters>,
    Path(volume_name): Path<String>,
) -> Result<()> {
    state
        .metastore
        .delete_volume(&volume_name, query.cascade.unwrap_or_default())
        .await
        .context(DeleteSnafu)?;
    Ok(())
}

#[utoipa::path(
    get,
    operation_id = "getVolumes",
    params(
        ("offset" = Option<usize>, Query, description = "Volumes offset"),
        ("limit" = Option<usize>, Query, description = "Volumes limit"),
        ("search" = Option<String>, Query, description = "Volumes search"),
        ("orderBy" = Option<String>, Query, description = "Order by: volume_name, volume_type, created_at (default), updated_at"),
        ("orderDirection" = Option<OrderDirection>, Query, description = "Order direction: ASC, DESC (default)"),
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
#[tracing::instrument(name = "api_ui::list_volumes", level = "info", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_volumes(
    DFSessionId(session_id): DFSessionId,
    Query(parameters): Query<SearchParameters>,
    State(state): State<AppState>,
) -> Result<Json<VolumesResponse>> {
    // let context = QueryContext::default();
    // let sql_string = "SELECT * FROM sqlite.meta.volumes".to_string();
    // let sql_string = apply_parameters(
    //     &sql_string,
    //     parameters,
    //     &["volume_name", "volume_type"],
    //     "created_at",
    //     OrderDirection::DESC,
    // );
    // let QueryResult { records, .. } = state
    //     .execution_svc
    //     .query(&session_id, sql_string.as_str(), context)
    //     .await
    //     .context(ListSnafu)?;
    // let mut items = Vec::new();
    // for record in records {
    //     let volume_names = downcast_string_column(&record, "volume_name").context(ListSnafu)?;
    //     let volume_types = downcast_string_column(&record, "volume_type").context(ListSnafu)?;
    //     let created_at_timestamps =
    //         downcast_string_column(&record, "created_at").context(ListSnafu)?;
    //     let updated_at_timestamps =
    //         downcast_string_column(&record, "updated_at").context(ListSnafu)?;
    //     for i in 0..record.num_rows() {
    //         items.push(Volume {
    //             name: volume_names.value(i).to_string(),
    //             r#type: volume_types.value(i).to_string(),
    //             created_at: created_at_timestamps.value(i).to_string(),
    //             updated_at: updated_at_timestamps.value(i).to_string(),
    //         });
    //     }
    // }
    // Ok(Json(VolumesResponse { items }))
    let items = state
        .metastore
        .get_volumes(parameters.into())
        .await
        .context(ListSnafu)?
        .into_iter()
        .map(|data| Volume::try_from(data).context(ListSnafu))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(Json(VolumesResponse { items }))
}
