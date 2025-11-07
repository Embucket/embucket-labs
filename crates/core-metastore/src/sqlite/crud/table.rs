use crate::error::{self as metastore_err, Result};
use crate::error::{SerdeSnafu};
use crate::models::RwObject;
use crate::models::{Table, TableId, SchemaId, DatabaseId, VolumeId};
use crate::models::{TableFormat, VolumeIdent, TableIdent};
use crate::sqlite::diesel_gen::tables;
use crate::SchemaIdent;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use validator::Validate;

#[derive(
    Validate, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Queryable, Selectable, Insertable,
)]
#[diesel(table_name = tables)]
#[diesel(belongs_to(Schema))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct TableRecord {
    pub id: i64,
    pub schema_id: i64,
    pub database_id: i64,
    pub volume_id: i64,
    pub name: String,
    pub metadata: String,
    pub metadata_location: String,
    pub properties: String,
    pub volume_location: Option<String>,
    pub is_temporary: bool,
    pub format: String,
    pub created_at: String, // if using TimestamptzSqlite it doen't support Eq
    pub updated_at: String,
}

impl TryFrom<RwObject<Table>> for TableRecord {
    type Error = metastore_err::Error;
    fn try_from(value: RwObject<Table>) -> Result<Self> {
        Ok(Self {
            // ignore missing id, maybe its insert, otherwise constraint will fail
            id: value.id().map_or(0, Into::into),
            schema_id: value.schema_id().map_or(0, Into::into),
            database_id: value.database_id().map_or(0, Into::into),
            volume_id: value.volume_id().map_or(0, Into::into),
            name: value.ident.to_string(),
            metadata: serde_json::to_string(&value.metadata)
                .context(SerdeSnafu)?,
            metadata_location: value.metadata_location.clone(),
            properties: serde_json::to_string(&value.properties)
                .context(SerdeSnafu)?,
            volume_location: value.volume_location.clone(),
            is_temporary: value.is_temporary,
            format: value.format.to_string(),
            created_at: value.created_at.to_rfc3339(),
            updated_at: value.updated_at.to_rfc3339(),
        })
    }
}

impl TryInto<RwObject<Table>> for (TableRecord, SchemaIdent, VolumeIdent) {
    type Error = metastore_err::Error;
    fn try_into(self) -> Result<RwObject<Table>> {
        let table = self.0;
        let SchemaIdent { schema, database } = self.1;
        let volume = self.2;
        // let volume_type = serde_json::from_str(&self.volume).context(SerdeSnafu)?;
        Ok(RwObject::new(Table {
            ident: TableIdent::new(
                &database,
                &schema,
                &table.name,
            ),
            metadata: serde_json::from_str(&table.metadata)
                .context(SerdeSnafu)?,
            metadata_location: table.metadata_location,
            properties: serde_json::from_str(&table.properties)
                .context(SerdeSnafu)?,
            volume_ident: Some(volume),     
            volume_location: table.volume_location,
            is_temporary: table.is_temporary,
            format: TableFormat::from(table.format),
        })
        .with_id(TableId(table.id))
        .with_schema_id(SchemaId(table.schema_id))
        .with_database_id(DatabaseId(table.database_id))
        .with_volume_id(VolumeId(table.volume_id))
        .with_created_at(
            DateTime::parse_from_rfc3339(&table.created_at)
                .context(metastore_err::TimeParseSnafu)?
                .with_timezone(&Utc),
        )
        .with_updated_at(
            DateTime::parse_from_rfc3339(&table.updated_at)
                .context(metastore_err::TimeParseSnafu)?
                .with_timezone(&Utc),
        ))
    }
}
