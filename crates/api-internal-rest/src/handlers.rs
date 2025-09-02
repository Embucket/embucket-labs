use super::error;
use super::error::Result;
use axum::{
    Json,
    extract::{Path, Query, State},
};
use snafu::ResultExt;

use core_history::{QueryIdParam, QueryRecord, QueryRecordId};
#[allow(clippy::wildcard_imports)]
use core_metastore::{
    error::{self as metastore_error},
    *,
};

use crate::{error::GetQuerySnafu, state::State as AppState};
use core_utils::scan_iterator::ScanIterator;
use validator::Validate;

pub type RwObjectVec<T> = Vec<RwObject<T>>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryParameters {
    #[serde(default)]
    pub cascade: Option<bool>,
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_volumes(State(state): State<AppState>) -> Result<Json<RwObjectVec<Volume>>> {
    let volumes = state
        .metastore
        .iter_volumes()
        .collect()
        .await
        .context(metastore_error::UtilSlateDBSnafu)
        .context(error::ListVolumesSnafu)?
        .iter()
        .map(|v| hide_sensitive(v.clone()))
        .collect();
    Ok(Json(volumes))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
) -> Result<Json<RwObject<Volume>>> {
    match state
        .metastore
        .get_volume(&volume_name)
        .await
        .context(error::GetVolumeSnafu)
    {
        Ok(Some(volume)) => Ok(Json(hide_sensitive(volume))),
        Ok(None) => metastore_error::VolumeNotFoundSnafu {
            volume: volume_name.clone(),
        }
        .fail()
        .context(error::GetVolumeSnafu),
        Err(error) => Err(error),
    }
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_volume(
    State(state): State<AppState>,
    Json(volume): Json<Volume>,
) -> Result<Json<RwObject<Volume>>> {
    volume
        .validate()
        .context(metastore_error::ValidationSnafu)
        .context(error::CreateVolumeSnafu)?;
    state
        .metastore
        .create_volume(&volume.ident.clone(), volume)
        .await
        .context(error::CreateVolumeSnafu)
        .map(|v| Json(hide_sensitive(v)))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn update_volume(
    State(state): State<AppState>,
    Path(volume_name): Path<String>,
    Json(volume): Json<Volume>,
) -> Result<Json<RwObject<Volume>>> {
    volume
        .validate()
        .context(metastore_error::ValidationSnafu)
        .context(error::UpdateVolumeSnafu)?;
    state
        .metastore
        .update_volume(&volume_name, volume)
        .await
        .context(error::UpdateVolumeSnafu)
        .map(|v| Json(hide_sensitive(v)))
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn delete_volume(
    State(state): State<AppState>,
    Query(query): Query<QueryParameters>,
    Path(volume_name): Path<String>,
) -> Result<()> {
    state
        .metastore
        .delete_volume(&volume_name, query.cascade.unwrap_or_default())
        .await
        .context(error::DeleteVolumeSnafu)
}

#[allow(clippy::needless_pass_by_value)]
#[must_use]
pub fn hide_sensitive(volume: RwObject<Volume>) -> RwObject<Volume> {
    let mut new_volume = volume;
    if let VolumeType::S3(ref mut s3_volume) = new_volume.data.volume
        && let Some(AwsCredentials::AccessKey(ref mut access_key)) = s3_volume.credentials
    {
        access_key.aws_access_key_id = "******".to_string();
        access_key.aws_secret_access_key = "******".to_string();
    }
    new_volume
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn list_databases(
    State(state): State<AppState>,
) -> Result<Json<Vec<RwObject<Database>>>> {
    state
        .metastore
        .iter_databases()
        .collect()
        .await
        .context(metastore_error::UtilSlateDBSnafu)
        .context(error::ListDatabasesSnafu)
        .map(Json)
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn get_database(
    State(state): State<AppState>,
    Path(database_name): Path<String>,
) -> Result<Json<RwObject<Database>>> {
    match state
        .metastore
        .get_database(&database_name)
        .await
        .context(error::GetDatabaseSnafu)
    {
        Ok(Some(db)) => Ok(Json(db)),
        Ok(None) => metastore_error::DatabaseNotFoundSnafu {
            db: database_name.clone(),
        }
        .fail()
        .context(error::GetDatabaseSnafu),
        Err(e) => Err(e),
    }
}

#[tracing::instrument(level = "debug", skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn create_database(
    State(state): State<AppState>,
    Json(database): Json<Database>,
) -> Result<Json<RwObject<Database>>> {
    database
        .validate()
        .context(metastore_error::ValidationSnafu)
        .context(error::CreateDatabaseSnafu)?;
    state
        .metastore
        .create_database(&database.ident.clone(), database)
        .await
        .context(error::CreateDatabaseSnafu)
        .map(Json)
}

#[tracing::instrument(level = "debug", fields(query_id), skip(state), err, ret(level = tracing::Level::TRACE))]
pub async fn query_by_id(
    State(state): State<AppState>,
    Path(query_id): Path<QueryIdParam>,
) -> Result<Json<RwObject<QueryRecord>>> {
    let query_id: QueryRecordId = query_id.into();
    let query_record = state
        .history_store
        .get_query(query_id)
        .await
        .context(GetQuerySnafu)?;

    Ok(Json(RwObject::new(query_record)))
}
