use diesel::prelude::*;
use diesel::query_dsl::methods::FindDsl;
use crate::models::Volume;
use crate::models::VolumeIdent;
use crate::models::RwObject;
use validator::Validate;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use diesel::sql_types::TimestamptzSqlite;
use uuid::Uuid;
use crate::sqlite::diesel_gen::volumes;
use crate::models::{Table};
use deadpool_diesel::sqlite::Pool;
use diesel::result::QueryResult;
use diesel::result::Error;
use crate::error::{self as metastore_err, Result};
use snafu::ResultExt;

#[derive(Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Queryable, Selectable, Insertable)]
#[serde(rename_all = "kebab-case")]
#[diesel(table_name = crate::sqlite::diesel_gen::volumes)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct VolumeRecord {
	pub id: String,
    pub ident: VolumeIdent,
    pub volume: String,
    pub created_at: String, // if using TimestamptzSqlite it doen't support Eq
    pub updated_at: String,
}

impl From<RwObject<Volume>> for VolumeRecord {
    fn from(value: RwObject<Volume>) -> Self {
        Self {
            id: value.id.to_string(),
            ident: value.ident.clone(),
            volume: serde_json::to_string(&value.volume).unwrap(),
            created_at: Utc::now().to_rfc3339(),
            updated_at: Utc::now().to_rfc3339(),
        }
    }
}

impl TryInto<RwObject<Volume>> for VolumeRecord {
    type Error = metastore_err::Error;
    fn try_into(self) -> Result<RwObject<Volume>> {
        Ok(RwObject {
            id: Uuid::parse_str(&self.id).context(metastore_err::UuidParseSnafu)?,
            data: Volume::new(self.ident, serde_json::from_str(&self.volume).unwrap()),
            created_at: DateTime::parse_from_rfc3339(&self.created_at).unwrap().with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339(&self.updated_at).unwrap().with_timezone(&Utc),
        })
    }
}

pub async fn create_volume(pool: &Pool, volume: RwObject<Volume>) -> Result<usize> {
    let volume = VolumeRecord::from(volume);
    let volume_name = volume.ident.clone();
    let conn = pool.get().await
        .context(metastore_err::DieselPoolSnafu)?;
    let create_volume_res = conn.interact(move |conn| -> QueryResult<usize> {
        diesel::insert_into(volumes::table)
            .values(&volume)
            .execute(conn)
    }).await?;
    if let Err(diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::UniqueViolation, _)) = create_volume_res {
        return metastore_err::VolumeAlreadyExistsSnafu{ volume: volume_name }.fail();
    }
    create_volume_res.context(metastore_err::DieselSnafu)
}

pub async fn get_volume(pool: &Pool, volume_ident: &VolumeIdent) -> Result<Option<RwObject<Volume>>> {
    let conn = pool.get().await?;
    let ident_owned = volume_ident.to_string();
    conn.interact(move |conn| -> QueryResult<Option<VolumeRecord>> {
        volumes::table
            .filter(volumes::ident.eq(ident_owned))
            .first::<VolumeRecord>(conn)
            .optional()
    }).await?
    .context(metastore_err::DieselSnafu)?
    .map(TryInto::try_into)
    .transpose()
}

pub async fn list_volumes(pool: &Pool) -> Result<Vec<RwObject<Volume>>> {
    let conn = pool.get().await?;
    // order by name to be compatible with previous slatedb metastore
    conn.interact(|conn| volumes::table.order(volumes::ident.asc()).load::<VolumeRecord>(conn))
        .await?
        .context(metastore_err::DieselSnafu)?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}

pub async fn update_volume(pool: &Pool, ident: &VolumeIdent, updated: Volume) -> Result<RwObject<Volume>> {
    let conn = pool.get().await?;
    let ident_owned = ident.to_string();
    let new_ident = updated.ident.to_string();
    conn.interact(move |conn| {
        diesel::update(volumes::table.filter(volumes::dsl::ident.eq(ident_owned)))
            .set(
                volumes::dsl::ident.eq(new_ident)
            )
            .returning(VolumeRecord::as_returning())
            .get_result(conn)
    })
    .await?
    .context(metastore_err::DieselSnafu)?
    .try_into()
}

pub async fn delete_volume(pool: &Pool, ident: &str) -> Result<RwObject<Volume>> {
    let conn = pool.get().await?;
    let ident_owned = ident.to_string();
    conn.interact(move |conn| {
        diesel::delete(volumes::table.filter(volumes::dsl::ident.eq(ident_owned)))
            .returning(VolumeRecord::as_returning())
            .get_result(conn)
    }).await?
    .context(metastore_err::DieselSnafu)?
    .try_into()
}
