#![allow(clippy::match_same_arms)]
use crate::Error;
use core_metastore::error::Error as MetastoreError;
use core_utils::Error as DbError;
use df_catalog::error::Error as CatalogError;
use iceberg_rust::error::Error as IcebergError;
use slatedb::SlateDBError;

#[derive(Debug, Eq, PartialEq)]
pub enum StatusCode {
    MetastoreError,
    ObjectStoreError,
    IcebergError,
    InternalError,
    OtherError,
}

pub trait IntoStatusCode {
    fn status_code(&self) -> StatusCode;
}

fn status_code_metastore(error: &MetastoreError) -> StatusCode {
    if let MetastoreError::UtilSlateDB { source, .. } = error {
        let error = source.as_ref();
        match error {
            DbError::Database { error, .. }
            | DbError::KeyGet { error, .. }
            | DbError::KeyDelete { error, .. }
            | DbError::KeyPut { error, .. }
            | DbError::ScanFailed { error, .. } => {
                if let SlateDBError::ObjectStoreError(_obj_store_error) = error {
                    StatusCode::ObjectStoreError
                } else {
                    StatusCode::MetastoreError
                }
            }
            _ => StatusCode::MetastoreError,
        }
    } else if let MetastoreError::ObjectStore { .. } = error {
        StatusCode::ObjectStoreError
    } else if let MetastoreError::Iceberg { error, .. } = error {
        let error = error.as_ref();
        if let IcebergError::External(err) = error {
            if err.downcast_ref::<object_store::Error>().is_some() {
                StatusCode::ObjectStoreError
            } else {
                StatusCode::IcebergError
            }
        } else {
            StatusCode::IcebergError
        }
    } else {
        StatusCode::OtherError
    }
}

// Select which status code to return.
impl IntoStatusCode for Error {
    #[allow(clippy::match_wildcard_for_single_variants)]
    #[allow(clippy::collapsible_match)]
    #[allow(clippy::if_same_then_else)]
    fn status_code(&self) -> StatusCode {
        match self {
            Self::CreateDatabase { source, .. } => {
                let source = source.as_ref();
                match source {
                    CatalogError::Metastore { source, .. } => status_code_metastore(source),
                    _ => StatusCode::MetastoreError,
                }
            }
            Self::Iceberg { error, .. } => {
                let error = error.as_ref();
                match error {
                    IcebergError::External(err) => {
                        // match volume communication errors
                        if err.downcast_ref::<object_store::Error>().is_some() {
                            StatusCode::ObjectStoreError
                        } else if let Some(error) = err.downcast_ref::<MetastoreError>() {
                            status_code_metastore(error)
                        } else {
                            StatusCode::IcebergError
                        }
                    }
                    _ => StatusCode::IcebergError,
                }
            }
            Self::Arrow { .. }
            | Self::SerdeParse { .. }
            | Self::CatalogListDowncast { .. }
            | Self::CatalogDownCast { .. }
            | Self::DataFusionLogicalPlanMergeTarget { .. }
            | Self::DataFusionLogicalPlanMergeSource { .. }
            | Self::DataFusionLogicalPlanMergeJoin { .. }
            | Self::LogicalExtensionChildCount { .. }
            | Self::MergeFilterStreamNotMatching { .. }
            | Self::MatchingFilesAlreadyConsumed { .. }
            | Self::MissingFilterPredicates { .. }
            | Self::RegisterCatalog { .. } => StatusCode::InternalError,
            _ => StatusCode::OtherError,
        }
    }
}
