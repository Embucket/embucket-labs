pub mod diesel_gen;
pub mod crud;

use diesel::sql_types::{Text};
use diesel::serialize::{ToSql, Output, IsNull};
use diesel::deserialize::{FromSql, Result};
use diesel::backend::{self, Backend};
use diesel::sqlite::Sqlite;
use crate::models::volumes::VolumeType;
use uuid::Uuid;

// impl ToSql<Text, Sqlite> for VolumeType {
//     fn to_sql<'b>(&self, out: &mut Output<'b, '_, Sqlite>) -> diesel::serialize::Result {
//         let s = serde_json::to_string(self)?;
//         out.set_value(s);
//         Ok(IsNull::No)
//     }
// }

// impl<DB, ST> FromSql<ST, DB> for VolumeType
// where
//     DB: Backend,
//     String: FromSql<ST, DB>,
// {
//     fn from_sql(bytes: DB::RawValue<'_>) -> diesel::deserialize::Result<Self> {
//         serde_json::from_str::<VolumeType>( &String::from_sql(bytes)? )
//             .map_err(Into::into)
//     }
// }
