use std::str::FromStr;

use diesel::prelude::*;
use diesel::query_dsl::methods::FindDsl;
use crate::models::{Volume, Database};
use crate::models::{VolumeIdent, DatabaseIdent};
use crate::models::RwObject;
use validator::Validate;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use diesel::sql_types::TimestamptzSqlite;
use uuid::Uuid;
use crate::sqlite::diesel_gen::{databases, volumes};
use crate::models::{Table};
use deadpool_diesel::sqlite::Pool;
use deadpool_diesel::sqlite::Connection;
use diesel::result::QueryResult;
use diesel::result::Error;
use crate::error::{self as metastore_err, Result};
use snafu::{ResultExt, OptionExt};
use crate::sqlite::crud::volumes::VolumeRecord;

// This intermediate struct is used for storage, though it is not used directly by the user (though it could)
// after it is loaded from sqlite it is converted to the RwObject<T> which we use as public interface.
// Fields order is matter and should match schema
#[derive(Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Queryable, Selectable, Insertable)]
#[serde(rename_all = "kebab-case")]
#[diesel(table_name = crate::sqlite::diesel_gen::databases)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DatabaseRecord {
    pub id: i64,    
    pub ident: DatabaseIdent,
    pub volume_id: i64,
    pub properties: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<RwObject<Database>> for DatabaseRecord {
    fn from(value: RwObject<Database>) -> Self {
        Self {
            id: value.id,
            ident: value.ident.clone(),
            volume_id: value.volume_id,
            properties: serde_json::to_string(&value.properties).ok(),
            created_at: Utc::now().to_rfc3339(),
            updated_at: Utc::now().to_rfc3339(),
        }
    }
}

impl TryInto<RwObject<Database>> for DatabaseRecord {
    type Error = metastore_err::Error;
    fn try_into(self) -> Result<RwObject<Database>> {
        let volume_id = self.volume_id;
        Ok(RwObject {
            id: self.id,
            data: Database::new(self.ident, volume_id),
            created_at: DateTime::parse_from_rfc3339(&self.created_at).unwrap().with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339(&self.updated_at).unwrap().with_timezone(&Utc),
        })
    }
}

fn lookup_volume(conn: &mut SqliteConnection, volume_ident: &str) -> Option<VolumeRecord> {
    volumes::table
        .filter(volumes::ident.eq(volume_ident))
        .first::<VolumeRecord>(conn)
        .ok()
}

pub async fn create_database(conn: &Connection, database: RwObject<Database>) -> Result<usize> {
    let database = DatabaseRecord::from(database);
    let db = database.ident.clone();
    let create_res = conn.interact(move |conn| -> QueryResult<usize> {
        diesel::insert_into(databases::table)
            .values(&database)
            .execute(conn)
    }).await?;
    if let Err(diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::UniqueViolation, _)) = create_res {
        return metastore_err::DatabaseAlreadyExistsSnafu{ db }.fail();
    }
    create_res.context(metastore_err::DieselSnafu)
}

pub async fn get_database(conn: &Connection, database_ident: &DatabaseIdent) -> Result<Option<RwObject<Database>>> {
    let ident_owned = database_ident.to_string();
    conn.interact(move |conn| -> QueryResult<Option<DatabaseRecord>> {
        databases::table
            .filter(databases::ident.eq(ident_owned))
            .select(DatabaseRecord::as_select())
            .first(conn)
            .optional()
    }).await?
    .context(metastore_err::DieselSnafu)?
    .map(TryInto::try_into)
    .transpose()
}

pub async fn list_databases(conn: &Connection, volume_id: Option<i64>) -> Result<Vec<RwObject<Database>>> {
    // order by name to be compatible with previous slatedb metastore
    conn.interact(move |conn| {
        if let Some(volume_id) = volume_id {
            databases::table
                .filter(databases::volume_id.eq(volume_id))
                .order(databases::ident.asc())
                .select(DatabaseRecord::as_select())
                .load::<DatabaseRecord>(conn)
        } else {
            databases::table
                .order(databases::ident.asc())
                .select(DatabaseRecord::as_select())
                .load::<DatabaseRecord>(conn)
        }
    }).await?
    .context(metastore_err::DieselSnafu)?
    .into_iter()
    .map(TryInto::try_into)
    .collect()
}

pub async fn update_database(conn: &Connection, ident: &VolumeIdent, updated: Database) -> Result<RwObject<Database>> {
    let ident_owned = ident.to_string();
    // DatabaseRecord (id, created_at, updated_at)  from converted item are fake and should not be used
    // nor returned, only needed to get converted to intermediate DatabaseRecord
    let updated = DatabaseRecord::from(RwObject::new(updated, None));
    conn.interact(move |conn| {
        diesel::update(databases::table.filter(databases::dsl::ident.eq(ident_owned)))
            .set((
                databases::dsl::ident.eq(updated.ident),
                databases::dsl::properties.eq(updated.properties),
                databases::dsl::volume_id.eq(updated.volume_id)))
            .returning(DatabaseRecord::as_returning())
            .get_result(conn)
    })
    .await?
    .context(metastore_err::DieselSnafu)?
    .try_into()
}

pub async fn delete_database_cascade(conn: &Connection, ident: &DatabaseIdent) -> Result<RwObject<Database>> {
    let ident_owned = ident.to_string();

    conn.interact(move |conn| {
        diesel::delete(databases::table.filter(databases::dsl::ident.eq(ident_owned)))
            .returning(DatabaseRecord::as_returning())
            .get_result(conn)
    }).await?
    .context(metastore_err::DieselSnafu)?
    .try_into()
}
