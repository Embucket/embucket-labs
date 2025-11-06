use std::str::FromStr;

use diesel::prelude::*;
use diesel::query_dsl::methods::FindDsl;
use crate::models::{Database, Schema};
use crate::models::{DatabaseIdent, SchemaIdent};
use crate::models::RwObject;
use validator::Validate;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use diesel::sql_types::TimestamptzSqlite;
use uuid::Uuid;
use crate::sqlite::diesel_gen::{databases, schemas};
use crate::models::{Table};
use deadpool_diesel::sqlite::Pool;
use deadpool_diesel::sqlite::Connection;
use diesel::result::QueryResult;
use diesel::result::Error;
use crate::error::{self as metastore_err, Result, SchemaNotFoundSnafu};
use snafu::{ResultExt, OptionExt};
use crate::sqlite::crud::databases::get_database;
use crate::{ListParams, OrderBy, OrderDirection};
use crate::sqlite::crud::current_ts_str;

// This intermediate struct is used for storage, though it is not used directly by the user (though it could)
// after it is loaded from sqlite it is converted to the RwObject<T> which we use as public interface.
// Fields order is matter and should match schema
#[derive(Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Queryable, Selectable, Insertable, Associations)]
#[serde(rename_all = "kebab-case")]
#[diesel(table_name = schemas)]
#[diesel(belongs_to(Database))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct SchemaRecord {
    pub id: i64,
    pub database_id: i64,
    pub name: String,
    pub properties: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl TryFrom<RwObject<Schema>> for SchemaRecord {
    type Error = metastore_err::Error;
    fn try_from(value: RwObject<Schema>) -> Result<Self> {
        Ok(Self {
            // ignore missing id, maybe its insert, otherwise constraint will fail
            id: value.id().unwrap_or_default(),
            database_id: value.database_id()?,
            name: value.ident.schema.clone(),
            properties: serde_json::to_string(&value.properties).ok(),
            created_at: value.created_at.to_rfc3339(),
            updated_at: value.updated_at.to_rfc3339(),
        })
    }
}

// SchemaRecord has no `volume_ident` field, so provide it as 2nd tuple item
impl TryInto<RwObject<Schema>> for (SchemaRecord, DatabaseIdent) {
    type Error = metastore_err::Error;
    fn try_into(self) -> Result<RwObject<Schema>> {
        let database_name = self.1;
        Ok(RwObject::new(Schema::new(
            SchemaIdent { schema: self.0.name, database: database_name }))
            .with_id(self.0.id)
            .with_database_id(self.0.database_id)
            .with_created_at(DateTime::parse_from_rfc3339(&self.0.created_at).unwrap().with_timezone(&Utc))
            .with_updated_at(DateTime::parse_from_rfc3339(&self.0.updated_at).unwrap().with_timezone(&Utc)))
    }
}

pub async fn create_schema(conn: &Connection, schema: RwObject<Schema>) -> Result<RwObject<Schema>> {
    let schema_ident = schema.ident.clone();
    let schema = SchemaRecord::try_from(schema)?;
    let create_res = conn.interact(move |conn| {
        diesel::insert_into(schemas::table)
            .values((
                schemas::name.eq(schema.name),
                schemas::database_id.eq(schema.database_id),
                schemas::properties.eq(schema.properties),
                schemas::created_at.eq(schema.created_at),
                schemas::updated_at.eq(schema.updated_at),
            ))
            .returning(SchemaRecord::as_returning())
            .get_result(conn)
    }).await?;
    tracing::info!("create_schema: {create_res:?}");
    if let Err(diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::UniqueViolation, _)) = create_res {
        return metastore_err::SchemaAlreadyExistsSnafu{
            db: schema_ident.database,
            schema: schema_ident.schema,
        }.fail();
    }
    create_res
        .context(metastore_err::DieselSnafu)
        .map(|r| (r, schema_ident.database))
        .and_then(TryInto::try_into)
}

pub async fn get_schema(conn: &Connection, schema_ident: &SchemaIdent) -> Result<Option<RwObject<Schema>>> {
    let mut items = list_schemas(
        conn, ListParams::default().with_name(schema_ident.schema.clone())).await?;
    if items.is_empty() {
        Ok(None)
    } else {
        Ok(Some(items.remove(0)))
    }
}

pub async fn get_schema_by_id(conn: &Connection, id: i64) -> Result<RwObject<Schema>> {
    let mut items = list_schemas(
        conn, ListParams::default().with_id(id)).await?;
    if items.is_empty() {
        SchemaNotFoundSnafu{ db: "", schema: format!("schemaId={id}") }.fail()
    } else {
        Ok(items.remove(0))
    }
}

pub async fn list_schemas(conn: &Connection, params: ListParams) -> Result<Vec<RwObject<Schema>>> {
    conn.interact(move |conn| {
        // map params to orm request in other way
        let mut query = schemas::table
            //  doing join to get database name
            .inner_join(databases::table.on(schemas::database_id.eq(databases::id)))
            .select((SchemaRecord::as_select(), databases::name))
            .into_boxed();
        
        if let Some(id) = params.id {
            query = query.filter(schemas::id.eq(id));
        }
        
        if let Some(database_id) = params.parent_id {
            query = query.filter(schemas::database_id.eq(database_id));
        }

        if let Some(search) = params.search {
            query = query.filter(schemas::name.like(format!("%{}%", search)));
        }

        if let Some(name) = params.name {
            query = query.filter(schemas::name.eq(name));
        }

        if let Some(parent_name) = params.parent_name {
            query = query.filter(databases::name.eq(parent_name));
        }

        if let Some(offset) = params.offset {
            query = query.offset(offset);
        }

        if let Some(limit) = params.limit {
            query = query.limit(limit);
        }

        for order_by in params.order_by {
            query = match order_by {
                OrderBy::Name(direction) => match direction {
                    OrderDirection::Desc => query.order(schemas::name.desc()),
                    OrderDirection::Asc => query.order(schemas::name.asc()),
                },
                // TODO: add parent name ordering (as separate function)
                OrderBy::ParentName(direction) => match direction {
                    OrderDirection::Desc => query.order(databases::name.desc()),
                    OrderDirection::Asc => query.order(databases::name.asc()),
                },                
                OrderBy::CreatedAt(direction) => match direction {
                    OrderDirection::Desc => query.order(schemas::created_at.desc()),
                    OrderDirection::Asc => query.order(schemas::created_at.asc()),
                },
                OrderBy::UpdatedAt(direction) => match direction {
                    OrderDirection::Desc => query.order(schemas::updated_at.desc()),
                    OrderDirection::Asc => query.order(schemas::updated_at.asc()),
                }
            }
        }

        query
            .load::<(SchemaRecord, String)>(conn)
    }).await?
    .context(metastore_err::DieselSnafu)?
    .into_iter()
    .map(TryInto::try_into)
    .collect()
}

pub async fn update_schema(conn: &Connection, ident: &SchemaIdent, updated: Schema) -> Result<RwObject<Schema>> {
    let database = get_database(conn, &ident.database)
        .await?
        .context(metastore_err::DatabaseNotFoundSnafu{ db: ident.database.clone() })?;
    let ident_owned = ident.clone();
    let database_id = database.id()?;

    // updated RwObject doesn't set (id, created_at, updated_at) fields, 
    // as it is only used for converting to a SchemaRecord
    let updated = SchemaRecord::try_from(RwObject::new(updated))?;

    conn.interact(move |conn| {
        diesel::update(schemas::table
            .filter(schemas::dsl::name.eq(ident_owned.schema)))
            .filter(schemas::dsl::database_id.eq(database_id))
            .set((
                schemas::dsl::name.eq(updated.name),
                schemas::dsl::properties.eq(updated.properties),
                schemas::dsl::updated_at.eq(current_ts_str())))
            .returning(SchemaRecord::as_returning())
            .get_result(conn)
    })
    .await?
    .map(|r| (r, ident.database.clone()))
    .context(metastore_err::DieselSnafu)?
    .try_into()
}

pub async fn delete_schema_cascade(conn: &Connection, ident: &SchemaIdent) -> Result<i64> {
    let database = get_database(conn, &ident.database)
        .await?
        .context(metastore_err::DatabaseNotFoundSnafu{ db: ident.database.clone() })?;
    let database_id = database.id()?;
    let ident_owned = ident.clone();

    conn.interact(move |conn| {
        diesel::delete(schemas::table
            .filter(schemas::dsl::name.eq(ident_owned.schema)))
            .filter(schemas::dsl::database_id.eq(database_id))
            .returning(schemas::id)
            .get_result(conn)
    }).await?
    .context(metastore_err::DieselSnafu)
}
