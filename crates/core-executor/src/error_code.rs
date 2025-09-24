use crate::error::OperationOn;
use crate::snowflake_error::Entity;
use std::fmt::Display;

// So far our ErrorCodes completely different from Snowflake error codes.
// For reference: https://github.com/snowflakedb/snowflake-cli/blob/main/src/snowflake/cli/api/errno.py
// Some of our error codes may be mapped to Snowflake error codes

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum ErrorCode {
    Db,
    Metastore,
    ObjectStore,
    Datafusion,
    DatafusionEmbucketFn,
    DatafusionEmbucketFnAggregate,
    DatafusionEmbucketFnConversion,
    DatafusionEmbucketFnDateTime,
    DatafusionEmbucketFnNumeric,
    DatafusionEmbucketFnSemiStructured,
    DatafusionEmbucketFnStringBinary,
    DatafusionEmbucketFnTable,
    DatafusionEmbucketFnCrate,
    DatafusionEmbucketFnRegexp,
    DatafusionEmbucketFnSystem,
    Arrow,
    Catalog,
    Iceberg,
    Internal,
    HistoricalQueryError,
    DataFusionSqlParse,
    DataFusionSql,
    EntityNotFound(Entity, OperationOn),
    Other,
    UnsupportedFeature,
}

impl Display for ErrorCode {
    #[allow(clippy::unnested_or_patterns)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = match self {
            Self::UnsupportedFeature => 2,
            Self::HistoricalQueryError => 1001,
            Self::DataFusionSqlParse => 1003,
            Self::DataFusionSql => 2003,
            Self::EntityNotFound(entity, operation) => match (entity, operation) {
                (Entity::Table, OperationOn::Table(..))
                | (Entity::Schema, OperationOn::Table(..))
                | (Entity::Database, OperationOn::Table(..)) => 2003,
                _ => 2043,
            },
            _ => 10001,
        };
        write!(f, "{code:06}")
    }
}
