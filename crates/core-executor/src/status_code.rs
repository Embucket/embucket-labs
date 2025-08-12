use crate::Error;
use super::snowflake_error::StatusCode;
use iceberg_rust::error::Error as IcebergError;
use slatedb::SlateDBError;
use core_metastore::error::Error as MetastoreError;
use core_utils::Error as DbError;

pub trait IntoStatusCode {
    fn status_code(&self) -> StatusCode;
    fn status_code_u16(&self) -> u16 {
        self.status_code().into()
    }
}

// Select which status code to return.
impl IntoStatusCode for Error {
    #[allow(clippy::match_wildcard_for_single_variants)]
    #[allow(clippy::collapsible_match)]
    fn status_code(&self) -> StatusCode {
        match self {
            Error::Iceberg { error, .. } => {
                let error = error.as_ref();
                match error {
                    IcebergError::External(err) => {
                        // match volume communication errors 
                        if err.downcast_ref::<object_store::Error>().is_some() {
                            StatusCode::ServiceUnavailable
                        } else if err.downcast_ref::<slatedb::SlateDBError>().is_some() {
                            StatusCode::ServiceUnavailable
                        } else if let Some(error) = err.downcast_ref::<MetastoreError>() {
                            if let MetastoreError::UtilSlateDB { source, .. } = error {
                                let error = source.as_ref();
                                match error {
                                    DbError::Database { error, .. } 
                                    | DbError::KeyGet { error, .. }
                                    | DbError::KeyDelete { error, .. }
                                    | DbError::KeyPut { error, .. }
                                    | DbError::ScanFailed { error, .. } => {
                                        if let SlateDBError::ObjectStoreError(_obj_store_error) = error {
                                            StatusCode::ServiceUnavailable
                                        } else {
                                            StatusCode::InternalServerError
                                        }
                                    },
                                    _ => StatusCode::InternalServerError,
                                }
                            } else if let MetastoreError::ObjectStore { .. } = error {
                                StatusCode::ServiceUnavailable
                            } else if let MetastoreError::Iceberg { error, .. } = error {
                                let error = error.as_ref();
                                if let IcebergError::External(err) = error {
                                    if err.downcast_ref::<object_store::Error>().is_some() {
                                        StatusCode::ServiceUnavailable
                                    } else {
                                        StatusCode::InternalServerError
                                    }
                                } else {
                                    StatusCode::InternalServerError
                                }
                            } else {
                                StatusCode::InternalServerError
                            }
                        } else {
                            StatusCode::InternalServerError
                        }
                    }
                    _ => StatusCode::InternalServerError,
                }
            },
            // Error::Metastore { source, .. } => {
            //     let source = source.as_ref();
            //     if let MetastoreError::UtilSlateDB { source, .. } = source {
            //         let source = source.as_ref();
            //         match source {
            //             DbError::Database { error, .. } 
            //             | DbError::KeyGet { error, .. }
            //             | DbError::KeyDelete { error, .. }
            //             | DbError::KeyPut { error, .. }
            //             | DbError::ScanFailed { error, .. } => {
            //                 if let SlateDBError::ObjectStoreError(_obj_store_error) = error {
            //                     StatusCode::ServiceUnavailable
            //                 } else {
            //                     StatusCode::InternalServerError
            //                 }
            //             },
            //             _ => StatusCode::InternalServerError,
            //         }
            //     } else {
            //         StatusCode::InternalServerError
            //     }
            // }
            Error::Arrow { .. }
            | Error::SerdeParse { .. }
            | Error::CatalogListDowncast { .. }
            | Error::CatalogDownCast { .. }
            | Error::DataFusionLogicalPlanMergeTarget { .. }
            | Error::DataFusionLogicalPlanMergeSource { .. }
            | Error::DataFusionLogicalPlanMergeJoin { .. }
            | Error::LogicalExtensionChildCount { .. }
            | Error::MergeFilterStreamNotMatching { .. }
            | Error::MatchingFilesAlreadyConsumed { .. }
            | Error::MissingFilterPredicates { .. }
            | Error::RegisterCatalog { .. } => StatusCode::InternalServerError,
            // => StatusCode::UNPROCESSABLE_ENTITY,
            _ => StatusCode::Ok,
        }
    }
}
