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
use crate::{ListParams, OrderBy, OrderDirection};

// This intermediate struct is used for storage, though it is not used directly by the user (though it could)
// after it is loaded from sqlite it is converted to the RwObject<T> which we use as public interface.
// Fields order is matter and should match schema
#[derive(Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Queryable, Selectable, Insertable, Associations)]
#[serde(rename_all = "kebab-case")]
#[diesel(table_name = crate::sqlite::diesel_gen::databases)]
#[diesel(belongs_to(Volume))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DatabaseRecord {
    pub id: i64,
    pub ident: DatabaseIdent,
    pub volume_id: i64,
    pub properties: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl TryFrom<RwObject<Database>> for DatabaseRecord {
    type Error = metastore_err::Error;
    fn try_from(value: RwObject<Database>) -> Result<Self> {
        Ok(Self {
            id: value.id()?,
            ident: value.ident.clone(),
            volume_id: value.volume_id()?,
            properties: serde_json::to_string(&value.properties).ok(),
            created_at: Utc::now().to_rfc3339(),
            updated_at: Utc::now().to_rfc3339(),
        })
    }
}

// DatabaseRecord has no `volume_ident` field, so provide it as 2nd tuple item
impl TryInto<RwObject<Database>> for (DatabaseRecord, VolumeIdent) {
    type Error = metastore_err::Error;
    fn try_into(self) -> Result<RwObject<Database>> {
        let volume_ident = self.1;
        Ok(RwObject::new(Database::new(self.0.ident, volume_ident))
            .with_id(self.0.id)
            .with_volume_id(self.0.volume_id)
            .with_created_at(DateTime::parse_from_rfc3339(&self.0.created_at).unwrap().with_timezone(&Utc))
            .with_updated_at(DateTime::parse_from_rfc3339(&self.0.updated_at).unwrap().with_timezone(&Utc)))
    }
}

// fn lookup_volume(conn: &mut SqliteConnection, volume_ident: &str) -> Option<VolumeRecord> {
//     volumes::table
//         .filter(volumes::ident.eq(volume_ident))
//         .first::<VolumeRecord>(conn)
//         .ok()
// }

pub async fn create_database(conn: &Connection, database: RwObject<Database>) -> Result<RwObject<Database>> {
    let database_ident = database.ident.clone();
    let volume_ident = database.volume.clone();
    let database = DatabaseRecord::try_from(database)?;
    let create_res = conn.interact(move |conn| {
        diesel::insert_into(databases::table)
            .values((
                databases::ident.eq(database.ident),
                databases::volume_id.eq(database.volume_id),
                databases::properties.eq(database.properties),
                databases::created_at.eq(database.created_at),
                databases::updated_at.eq(database.updated_at),
            ))
            .returning(DatabaseRecord::as_returning())
            .get_result(conn)
    }).await?;
    tracing::info!("create_database: {create_res:?}");
    if let Err(diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::UniqueViolation, _)) = create_res {
        return metastore_err::DatabaseAlreadyExistsSnafu{ db: database_ident }.fail();
    }
    create_res
        .context(metastore_err::DieselSnafu)
        .map(|r| (r, volume_ident))
        .and_then(TryInto::try_into)
}

pub async fn get_database(conn: &Connection, database_ident: &DatabaseIdent) -> Result<Option<RwObject<Database>>> {
    let ident_owned = database_ident.to_string();
    conn.interact(move |conn| -> QueryResult<Option<(DatabaseRecord, VolumeIdent)>> {
        databases::table
            .inner_join(volumes::table.on(databases::volume_id.eq(volumes::id)))
            .filter(databases::ident.eq(ident_owned))
            .select((DatabaseRecord::as_select(), volumes::ident))
            .first(conn)
            .optional()
    }).await?
    .context(metastore_err::DieselSnafu)?
    .map(TryInto::try_into)
    .transpose()
}

pub async fn list_databases(conn: &Connection, params: ListParams) -> Result<Vec<RwObject<Database>>> {
    // TODO: add filtering, ordering params
    conn.interact(move |conn| {
        // map params to orm request in other way
        let mut query = databases::table.into_boxed();
        if let Some(volume_id) = params.parent_id {
            query = query.filter(databases::volume_id.eq(volume_id));
        }

        if let Some(offset) = params.offset {
            query = query.offset(offset);
        }

        if let Some(limit) = params.limit {
            query = query.limit(limit);
        }

        if let Some(search) = params.search {
            query = query.filter(databases::ident.like(format!("%{}%", search)));
        }

        for order_by in params.order_by {
            query = match order_by {
                OrderBy::Name(direction) => match direction {
                    OrderDirection::Desc => query.order(databases::ident.desc()),
                    OrderDirection::Asc => query.order(databases::ident.asc()),
                },
                // TODO: add parent name ordering (as separate function)
                OrderBy::ParentName(direction) => match direction {
                    OrderDirection::Desc => query.order(databases::ident.desc()),
                    OrderDirection::Asc => query.order(databases::ident.asc()),
                },                
                OrderBy::CreatedAt(direction) => match direction {
                    OrderDirection::Desc => query.order(databases::created_at.desc()),
                    OrderDirection::Asc => query.order(databases::created_at.asc()),
                },
                OrderBy::UpdatedAt(direction) => match direction {
                    OrderDirection::Desc => query.order(databases::updated_at.desc()),
                    OrderDirection::Asc => query.order(databases::updated_at.asc()),
                }
            }
        }

        query
            .inner_join(volumes::table.on(databases::volume_id.eq(volumes::id)))
            .select((DatabaseRecord::as_select(), volumes::ident))
            .load::<(DatabaseRecord, String)>(conn)
    }).await?
    .context(metastore_err::DieselSnafu)?
    .into_iter()
    .map(TryInto::try_into)
    .collect()
}

pub async fn update_database(conn: &Connection, ident: &DatabaseIdent, updated: Database) -> Result<RwObject<Database>> {
    let ident_owned = ident.to_string();
    let volume_ident = updated.volume.clone();
    // updated RwObject didn't set (id, created_at, updated_at) fields, 
    // as it is only used for converting to a DatabaseRecord
    let updated = DatabaseRecord::try_from(RwObject::new(updated))?;
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
    .map(|r| (r, volume_ident))
    .context(metastore_err::DieselSnafu)?
    .try_into()
}

pub async fn delete_database_cascade(conn: &Connection, ident: &DatabaseIdent) -> Result<i64> {
    let ident_owned = ident.to_string();

    conn.interact(move |conn| {
        diesel::delete(databases::table.filter(databases::dsl::ident.eq(ident_owned)))
            .returning(databases::id)
            .get_result(conn)
    }).await?
    .context(metastore_err::DieselSnafu)
}
