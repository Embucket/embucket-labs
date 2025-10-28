// use diesel::prelude::*;
// use crate::sqlite::diesel_gen::tables::dsl::*;
// use crate::models::{Table};
// use deadpool_diesel::sqlite::Pool;
// use diesel::result::Error;
// use crate::error::*;



// pub async fn create_table(pool: &Pool, new_table: NewTable) -> Result<()> {
//     let conn = pool.get().await;
//     conn.interact(move |conn| {
//         diesel::insert_into(tables)
//             .values(&new_table)
//             .execute(conn)
//     }).await?
// }

// pub async fn get_table(pool: &Pool, table_ident: &str) -> Result<Option<Table>, Error> {
//     let conn = pool.get().await?;
//     let ident_owned = table_ident.to_string();
//     conn.interact(move |conn| {
//         tables
//             .filter(ident.eq(ident_owned))
//             .first::<Table>(conn)
//             .optional()
//     }).await?
// }

// pub async fn list_tables(pool: &Pool) -> Result<Vec<Table>, Error> {
//     let conn = pool.get().await?;
//     conn.interact(|conn| tables.load::<Table>(conn)).await?
// }

// pub async fn update_table(pool: &Pool, updated: Table) -> Result<(), Error> {
//     let conn = pool.get().await?;
//     let id = updated.ident.clone();
//     conn.interact(move |conn| {
//         diesel::update(tables.filter(ident.eq(id)))
//             .set((
//                 metadata.eq(updated.metadata),
//                 metadata_location.eq(updated.metadata_location),
//                 properties.eq(updated.properties),
//                 volume_ident.eq(updated.volume_ident),
//                 volume_location.eq(updated.volume_location),
//                 is_temporary.eq(updated.is_temporary),
//                 format.eq(updated.format),
//             ))
//             .execute(conn)
//     }).await?
// }

// pub async fn delete_table(pool: &Pool, table_ident: &str) -> Result<(), Error> {
//     let conn = pool.get().await?;
//     let ident_owned = table_ident.to_string();
//     conn.interact(move |conn| {
//         diesel::delete(tables.filter(ident.eq(ident_owned)))
//             .execute(conn)
//     }).await?
// }
