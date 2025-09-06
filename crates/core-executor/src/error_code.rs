use serde::Serialize;
use std::fmt::Display;

// Our ErrorCodes completely different from Snowflake error codes.
// For reference: https://github.com/snowflakedb/snowflake-cli/blob/main/src/snowflake/cli/api/errno.py
// Some of our error codes may be mapped to Snowflake error codes

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize)]
pub enum ErrorCode {
    Db,
    Metastore = 101,
    ObjectStore = 201,
    Datafusion = 301,
    DatafusionEmbucketFn = 501,
    DatafusionEmbucketFnAggregate,
    DatafusionEmbucketFnConversion,
    DatafusionEmbucketFnDateTime,
    DatafusionEmbucketFnNumeric,
    DatafusionEmbucketFnSemiStructured,
    DatafusionEmbucketFnStringBinary,
    DatafusionEmbucketFnTable,
    DatafusionEmbucketFnCrate,
    DatafusionEmbucketFnRegexp,
    Arrow = 601,
    Catalog = 701,
    Iceberg = 801,
    Internal = 901,
    #[serde(rename = "001001")]
    HistoricalQueryError,

    #[serde(rename = "001003")]
    DataFusionSqlParse,

    #[serde(rename = "002003")]
    DataFusionSql,

    #[serde(rename = "002043")]
    TableNotFound,
    #[serde(rename = "002043")]
    SchemaNotFound,
    #[serde(rename = "002043")]
    DatabaseNotFound,
    Other = 10001,
    UnsupportedFeature,
}

impl From<ErrorCode> for u32 {
    #[allow(clippy::as_conversions)]
    fn from(status_code: ErrorCode) -> Self {
        status_code as Self
    }
}

// Following Display fmt is using for integer output, since we use serde to attach arbitrary value
// to every enum, by convention it is integer value. After serializing check if value in double quotes
// can be converted to integer, if not then use enum value.
// By default serializer will serialize enum name, which we don't need.
impl Display for ErrorCode {
    #[allow(clippy::as_conversions)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // check if serializable value has starting, traling double quotes
        let serialized = serde_json::to_string(self).unwrap_or_default();
        if serialized.starts_with('"') && serialized.ends_with('"') {
            // fetch value between double quotes
            let value = &serialized[1..serialized.len() - 1];
            let parsed = value.parse::<u32>();
            if let Ok(parsed) = parsed {
                return parsed.fmt(f);
            }
        }
        // serde serialized just name of enum variant, get number instead
        write!(f, "{}", *self as u32)
    }
}
