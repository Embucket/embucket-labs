use diesel::prelude::*;
use diesel::query_dsl::methods::FindDsl;
use crate::models::Volume;
use crate::models::VolumeIdent;
use crate::models::RwObject;
use crate::sqlite::crud::databases::list_databases;
use validator::Validate;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use diesel::sql_types::TimestamptzSqlite;
use uuid::Uuid;
use crate::sqlite::diesel_gen::volumes;
use crate::sqlite::diesel_gen::databases;
use crate::models::{Table};
use deadpool_diesel::sqlite::Connection;
use diesel::result::QueryResult;
use diesel::result::Error;
use crate::error::{self as metastore_err, Result};
use snafu::{ResultExt, OptionExt};

#[derive(Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Queryable, Selectable, Insertable)]
#[serde(rename_all = "kebab-case")]
#[diesel(table_name = crate::sqlite::diesel_gen::volumes)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct VolumeRecord {
	pub id: i64,
    pub ident: VolumeIdent,
    pub volume: String,
    pub created_at: String, // if using TimestamptzSqlite it doen't support Eq
    pub updated_at: String,
}

impl From<RwObject<Volume>> for VolumeRecord {
    fn from(value: RwObject<Volume>) -> Self {
        Self {
            id: value.id,
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
            id: self.id,
            // todo: replace unwrap by fallible conversion
            data: Volume::new(self.ident, serde_json::from_str(&self.volume).unwrap()),
            created_at: DateTime::parse_from_rfc3339(&self.created_at).unwrap().with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339(&self.updated_at).unwrap().with_timezone(&Utc),
        })
    }
}

pub async fn create_volume(conn: &Connection, volume: RwObject<Volume>) -> Result<usize> {
    let volume = VolumeRecord::from(volume);
    let volume_name = volume.ident.clone();
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

pub async fn get_volume(conn: &Connection, volume_ident: &VolumeIdent) -> Result<Option<RwObject<Volume>>> {
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

pub async fn get_volume_by_id(conn: &Connection, volume_id: i64) -> Result<Option<RwObject<Volume>>> {
    conn.interact(move |conn| -> QueryResult<Option<VolumeRecord>> {
        volumes::table
            .filter(volumes::id.eq(volume_id))
            .first::<VolumeRecord>(conn)
            .optional()
    }).await?
    .context(metastore_err::DieselSnafu)?
    .map(TryInto::try_into)
    .transpose()
}

pub async fn list_volumes(conn: &Connection) -> Result<Vec<RwObject<Volume>>> {
    // order by name to be compatible with previous slatedb metastore
    conn.interact(|conn| volumes::table.order(volumes::ident.asc()).load::<VolumeRecord>(conn))
        .await?
        .context(metastore_err::DieselSnafu)?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}

// Only rename volume is supported
pub async fn update_volume(conn: &Connection, ident: &VolumeIdent, updated: Volume) -> Result<RwObject<Volume>> {
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

pub async fn delete_volume_cascade(conn: &Connection, ident: &VolumeIdent) -> Result<RwObject<Volume>> {
    let ident_owned = ident.to_string();
    conn.interact(move |conn| {
        diesel::delete(volumes::table.filter(volumes::dsl::ident.eq(ident_owned)))
            .returning(VolumeRecord::as_returning())
            .get_result(conn)
    }).await?
    .context(metastore_err::DieselSnafu)?
    .try_into()
}
