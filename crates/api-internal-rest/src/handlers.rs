use super::error::{MetastoreAPIError, MetastoreAPIResult};
use axum::{
    Json,
    extract::{Path, Query, State},
};
use snafu::ResultExt;

#[allow(clippy::wildcard_imports)]
use core_metastore::{
    error::{self as metastore_error, MetastoreError},
    *,
};

use crate::state::State as AppState;
use core_utils::scan_iterator::ScanIterator;
use validator::Validate;

pub type RwObjectVec<T> = Vec<RwObject<T>>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryParameters {
    #[serde(default)]
    pub cascade: Option<bool>,
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_volumes(
    State(state): State<AppState>,
) -> MetastoreAPIResult<Json<RwObjectVec<Volume>>> {
    let volumes = state
        .metastore
        .iter_volumes()
        .collect()
        .await
        .map_err(|e| MetastoreAPIError(MetastoreError::UtilSlateDB { source: e }))?
        .iter()
        .map(|v| hide_sensitive(v.clone()))
        .collect();
    Ok(Json(volumes))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
) -> MetastoreAPIResult<Json<RwObject<Volume>>> {
    match state.metastore.get_volume(&volume_name).await {
        Ok(Some(volume)) => Ok(Json(hide_sensitive(volume))),
        Ok(None) => Err(MetastoreError::VolumeNotFound {
            volume: volume_name.clone(),
        }
        .into()),
        Err(e) => Err(e.into()),
    }
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_volume(
    State(state): State<AppState>,
    Json(volume): Json<Volume>,
) -> MetastoreAPIResult<Json<RwObject<Volume>>> {
    volume
        .validate()
        .context(metastore_error::ValidationSnafu)?;
    state
        .metastore
        .create_volume(&volume.ident.clone(), volume)
        .await
        .map_err(MetastoreAPIError)
        .map(|v| Json(hide_sensitive(v)))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
    Json(volume): Json<Volume>,
) -> MetastoreAPIResult<Json<RwObject<Volume>>> {
    volume
        .validate()
        .context(metastore_error::ValidationSnafu)?;
    state
        .metastore
        .update_volume(&volume_name, volume)
        .await
        .map_err(MetastoreAPIError)
        .map(|v| Json(hide_sensitive(v)))
}

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

#[allow(clippy::needless_pass_by_value)]
#[must_use]
pub fn hide_sensitive(volume: RwObject<Volume>) -> RwObject<Volume> {
    let mut new_volume = volume;
    if let VolumeType::S3(ref mut s3_volume) = new_volume.data.volume {
        if let Some(AwsCredentials::AccessKey(ref mut access_key)) = s3_volume.credentials {
            access_key.aws_access_key_id = "******".to_string();
            access_key.aws_secret_access_key = "******".to_string();
        }
    }
    new_volume
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_databases(
    State(state): State<AppState>,
) -> MetastoreAPIResult<Json<Vec<RwObject<Database>>>> {
    state
        .metastore
        .iter_databases()
        .collect()
        .await
        .map_err(|e| MetastoreAPIError(MetastoreError::UtilSlateDB { source: e }))
        .map(Json)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_database(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> MetastoreAPIResult<Json<RwObject<Database>>> {
    match state.metastore.get_database(&database_name).await {
        Ok(Some(db)) => Ok(Json(db)),
        Ok(None) => Err(MetastoreError::DatabaseNotFound {
            db: database_name.clone(),
        }
        .into()),
        Err(e) => Err(e.into()),
    }
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_database(
    State(state): State<AppState>,
    Json(database): Json<Database>,
) -> MetastoreAPIResult<Json<RwObject<Database>>> {
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
