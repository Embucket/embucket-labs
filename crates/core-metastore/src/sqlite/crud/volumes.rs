use crate::error::{self as metastore_err, Result};
use crate::error::{SerdeSnafu, VolumeNotFoundSnafu};
use crate::models::RwObject;
use crate::models::Volume;
use crate::models::{DatabaseIdent, VolumeId, VolumeIdent};
use crate::sqlite::crud::current_ts_str;
use crate::sqlite::diesel_gen::databases;
use crate::sqlite::diesel_gen::volumes;
use crate::{ListParams, OrderBy, OrderDirection};
use chrono::{DateTime, Utc};
use deadpool_diesel::sqlite::Connection;
use diesel::prelude::*;
use diesel::result::QueryResult;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use validator::Validate;

#[derive(
    Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Queryable, Selectable, Insertable,
)]
#[diesel(table_name = volumes)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct VolumeRecord {
    pub id: i64,
    pub name: String,
    pub volume_type: String, // display name
    pub volume: String,
    pub created_at: String, // if using TimestamptzSqlite it doen't support Eq
    pub updated_at: String,
}

impl TryFrom<RwObject<Volume>> for VolumeRecord {
    type Error = metastore_err::Error;
    fn try_from(value: RwObject<Volume>) -> Result<Self> {
        Ok(Self {
            // ignore missing id, maybe its insert, otherwise constraint will fail
            id: value.id().map_or(0, Into::into),
            name: value.ident.clone(),
            volume_type: value.volume.to_string(), // display name
            volume: serde_json::to_string(&value.volume).context(SerdeSnafu)?,
            created_at: value.created_at.to_rfc3339(),
            updated_at: value.updated_at.to_rfc3339(),
        })
    }
}

impl TryInto<RwObject<Volume>> for VolumeRecord {
    type Error = metastore_err::Error;
    fn try_into(self) -> Result<RwObject<Volume>> {
        let volume_type = serde_json::from_str(&self.volume).context(SerdeSnafu)?;
        Ok(RwObject::new(Volume::new(self.name, volume_type))
            .with_id(VolumeId(self.id))
            .with_created_at(
                DateTime::parse_from_rfc3339(&self.created_at)
                    .context(metastore_err::TimeParseSnafu)?
                    .with_timezone(&Utc),
            )
            .with_updated_at(
                DateTime::parse_from_rfc3339(&self.updated_at)
                    .context(metastore_err::TimeParseSnafu)?
                    .with_timezone(&Utc),
            ))
    }
}

pub async fn create_volume(
    conn: &Connection,
    volume: RwObject<Volume>,
) -> Result<RwObject<Volume>> {
    let volume = VolumeRecord::try_from(volume)?;
    let volume_name = volume.name.clone();
    let create_volume_res = conn
        .interact(move |conn| -> QueryResult<VolumeRecord> {
            diesel::insert_into(volumes::table)
                // prepare values explicitely to filter out id
                .values((
                    volumes::name.eq(volume.name),
                    volumes::volume_type.eq(volume.volume_type),
                    volumes::volume.eq(volume.volume),
                    volumes::created_at.eq(volume.created_at),
                    volumes::updated_at.eq(volume.updated_at),
                ))
                .returning(VolumeRecord::as_returning())
                .get_result(conn)
        })
        .await?;
    if let Err(diesel::result::Error::DatabaseError(
        diesel::result::DatabaseErrorKind::UniqueViolation,
        _,
    )) = create_volume_res
    {
        return metastore_err::VolumeAlreadyExistsSnafu {
            volume: volume_name,
        }
        .fail();
    }
    create_volume_res
        .context(metastore_err::DieselSnafu)?
        .try_into()
}

pub async fn get_volume(
    conn: &Connection,
    volume_ident: &VolumeIdent,
) -> Result<Option<RwObject<Volume>>> {
    let mut items = list_volumes(conn, ListParams::default().by_name(volume_ident.clone())).await?;
    if items.is_empty() {
        VolumeNotFoundSnafu {
            volume: volume_ident.clone(),
        }
        .fail()
    } else {
        Ok(Some(items.remove(0)))
    }
}

pub async fn get_volume_by_id(conn: &Connection, volume_id: VolumeId) -> Result<RwObject<Volume>> {
    let mut items = list_volumes(conn, ListParams::default().by_id(*volume_id)).await?;
    if items.is_empty() {
        VolumeNotFoundSnafu {
            volume: volume_id.to_string(),
        }
        .fail()
    } else {
        Ok(items.remove(0))
    }
}

pub async fn get_volume_by_database(
    conn: &Connection,
    database_name: DatabaseIdent,
) -> Result<Option<RwObject<Volume>>> {
    conn.interact(move |conn| -> QueryResult<Option<VolumeRecord>> {
        volumes::table
            .inner_join(databases::table.on(databases::volume_id.eq(volumes::id)))
            .filter(databases::name.eq(database_name))
            .select(VolumeRecord::as_select())
            .first::<VolumeRecord>(conn)
            .optional()
    })
    .await?
    .context(metastore_err::DieselSnafu)?
    .map(TryInto::try_into)
    .transpose()
}

pub async fn list_volumes(conn: &Connection, params: ListParams) -> Result<Vec<RwObject<Volume>>> {
    // TODO: add filtering, ordering params
    conn.interact(move |conn| {
        // map params to orm request in other way
        let mut query = volumes::table.into_boxed();

        if let Some(id) = params.id {
            query = query.filter(volumes::id.eq(id));
        }

        if let Some(search) = params.search {
            query = query.filter(volumes::name.like(format!("%{search}%")));
        }

        if let Some(name) = params.name {
            query = query.filter(volumes::name.eq(name));
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
                    OrderDirection::Desc => query.order(volumes::name.desc()),
                    OrderDirection::Asc => query.order(volumes::name.asc()),
                },
                // TODO: add parent name ordering (as separate function)
                OrderBy::ParentName(_) => {
                    tracing::warn!("ParentName ordering is not supported for volumes");
                    query
                }
                OrderBy::CreatedAt(direction) => match direction {
                    OrderDirection::Desc => query.order(volumes::created_at.desc()),
                    OrderDirection::Asc => query.order(volumes::created_at.asc()),
                },
                OrderBy::UpdatedAt(direction) => match direction {
                    OrderDirection::Desc => query.order(volumes::updated_at.desc()),
                    OrderDirection::Asc => query.order(volumes::updated_at.asc()),
                },
            }
        }

        query
            .select(VolumeRecord::as_select())
            .load::<VolumeRecord>(conn)
    })
    .await?
    .context(metastore_err::DieselSnafu)?
    .into_iter()
    .map(TryInto::try_into)
    .collect()
}

// Only rename volume is supported
pub async fn update_volume(
    conn: &Connection,
    ident: &VolumeIdent,
    updated: Volume,
) -> Result<RwObject<Volume>> {
    let ident_owned = ident.clone();
    let new_ident = updated.ident.clone();
    conn.interact(move |conn| {
        diesel::update(volumes::table.filter(volumes::dsl::name.eq(ident_owned)))
            .set((
                // for volumes only rename, updated_at fields can be changed
                volumes::dsl::name.eq(new_ident),
                volumes::dsl::updated_at.eq(current_ts_str()),
            ))
            .returning(VolumeRecord::as_returning())
            .get_result(conn)
    })
    .await?
    .context(metastore_err::DieselSnafu)?
    .try_into()
}

pub async fn delete_volume_cascade(
    conn: &Connection,
    ident: &VolumeIdent,
) -> Result<RwObject<Volume>> {
    let ident_owned = ident.clone();
    conn.interact(move |conn| {
        diesel::delete(volumes::table.filter(volumes::dsl::name.eq(ident_owned)))
            .returning(VolumeRecord::as_returning())
            .get_result(conn)
    })
    .await?
    .context(metastore_err::DieselSnafu)?
    .try_into()
}
