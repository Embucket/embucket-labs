use std::fmt::Display;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum StatusCode {
    Db,
    Metastore = 101,
    NotFound = 2043,
    ObjectStore = 201,
    Datafusion = 301,
    DataFusionSql = 401,
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
    Other = 10001,
    UnsupportedFeature,
}

impl From<StatusCode> for u32 {
    #[allow(clippy::as_conversions)]
    fn from(status_code: StatusCode) -> Self {
        status_code as Self
    }
}

impl Display for StatusCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:06}", u32::from(*self))
    }
}
