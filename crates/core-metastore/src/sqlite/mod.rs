pub mod crud;
pub mod diesel_gen;

use crate::Result;
use crate::error::SqlSnafu;
use deadpool_sqlite::Object;
use rusqlite::Result as SqlResult;
use snafu::ResultExt;

#[derive(Debug, Clone)]
pub struct Stats {
    pub total_volumes: usize,
    pub total_databases: usize,
    pub total_schemas: usize,
    pub total_tables: usize,
}

pub async fn get_stats(connection: &Object) -> Result<Stats> {
    let sql = "
    SELECT
        COUNT(DISTINCT v.id) AS volume_count,
        COUNT(DISTINCT d.id) AS database_count,
        COUNT(DISTINCT s.id) AS schema_count,
        COUNT(DISTINCT t.id) AS table_count
    FROM
        volumes v
    LEFT JOIN databases d ON d.volume_id = v.id
    LEFT JOIN schemas s ON s.database_id = d.id
    LEFT JOIN tables t ON t.schema_id = s.id;";

    let stats = connection
        .interact(move |conn| -> SqlResult<Stats> {
            conn.query_row(sql, [], |row| {
                let total_volumes = row.get::<_, usize>(0)?;
                let total_databases = row.get::<_, usize>(1)?;
                let total_schemas = row.get::<_, usize>(2)?;
                let total_tables = row.get::<_, usize>(3)?;
                Ok(Stats {
                    total_volumes,
                    total_databases,
                    total_schemas,
                    total_tables,
                })
            })
        })
        .await?
        .context(SqlSnafu)?;

    Ok(stats)
}
