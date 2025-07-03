#![allow(clippy::redundant_else)]
use crate::error::Error;
use core_metastore::error::Error as MetastoreError;
use datafusion_common::error::DataFusionError;
use df_catalog::df_error::DFExternalError as DFCatalogExternalDFError;
use embucket_functions::df_error::DFExternalError as EmubucketFunctionsExternalDFError;
use snafu::Snafu;

#[derive(Snafu, Debug)]
#[snafu(display("{message}"))]
pub struct SnowflakeError {
    pub message: String,
}

impl From<Error> for SnowflakeError {
    fn from(value: Error) -> Self {
        let message = value.to_string();
        match value {
            Error::RegisterUDF { error, .. }
            | Error::RegisterUDAF { error, .. }
            | Error::DataFusionQuery { error, .. }
            | Error::DataFusion { error, .. } => match *error {
                DataFusionError::ArrowError { .. } => Self {
                    message: error.to_string(),
                },
                DataFusionError::External(err) => {
                    if err.is::<DataFusionError>() {
                        if let Ok(e) = err.downcast::<DataFusionError>() {
                            let err = *e;
                            let message = err.to_string();
                            match err {
                                DataFusionError::ArrowError(_arrow_error, Some(_backtrace)) => {
                                    Self { message }
                                }
                                DataFusionError::Collection(_df_errors) => Self { message },
                                DataFusionError::Context(_context, _inner) => Self { message },
                                DataFusionError::Diagnostic(_diagnostic, _inner) => {
                                    Self { message }
                                }
                                DataFusionError::Execution(_execution_error) => Self { message },
                                DataFusionError::IoError(_io_error) => Self { message },
                                DataFusionError::NotImplemented(_not_implemented_error) => {
                                    Self { message }
                                }
                                DataFusionError::ObjectStore(_object_store_error) => {
                                    Self { message }
                                }
                                DataFusionError::ParquetError(_parquet_error) => Self { message },
                                DataFusionError::SchemaError(_schema_error, _boxed_backtrace) => {
                                    Self { message }
                                }
                                DataFusionError::Shared(_shared_error) => Self { message },
                                DataFusionError::SQL(_sql_error, Some(_backtrace)) => {
                                    Self { message }
                                }
                                DataFusionError::SQL(_sql_error, None) => Self { message },
                                DataFusionError::Substrait(_substrait_error) => Self { message },
                                DataFusionError::External(_external_error) => Self { message },
                                DataFusionError::Internal(_internal_error) => Self { message },
                                _ => Self { message },
                            }
                        } else {
                            unreachable!()
                        }
                    } else if err.is::<EmubucketFunctionsExternalDFError>() {
                        if let Ok(e) = err.downcast::<EmubucketFunctionsExternalDFError>() {
                            let e = *e;
                            let message = e.to_string();
                            match e {
                                EmubucketFunctionsExternalDFError::Aggregate { source } => {
                                    return Self {
                                        message: source.to_string(),
                                    };
                                }
                                EmubucketFunctionsExternalDFError::Conversion { source } => {
                                    return Self {
                                        message: source.to_string(),
                                    };
                                }
                                EmubucketFunctionsExternalDFError::DateTime { source } => {
                                    return Self {
                                        message: source.to_string(),
                                    };
                                }
                                EmubucketFunctionsExternalDFError::Numeric { source } => {
                                    return Self {
                                        message: source.to_string(),
                                    };
                                }
                                EmubucketFunctionsExternalDFError::SemiStructured { source } => {
                                    return Self {
                                        message: source.to_string(),
                                    };
                                }
                                EmubucketFunctionsExternalDFError::StringBinary { source } => {
                                    return Self {
                                        message: source.to_string(),
                                    };
                                }
                                EmubucketFunctionsExternalDFError::Table { source } => {
                                    return Self {
                                        message: source.to_string(),
                                    };
                                }
                                EmubucketFunctionsExternalDFError::Crate { source } => {
                                    return Self {
                                        message: source.to_string(),
                                    };
                                }
                                _ => return Self { message },
                            }
                        } else {
                            unreachable!()
                        }
                    } else if err.is::<DFCatalogExternalDFError>() {
                        if let Ok(e) = err.downcast::<DFCatalogExternalDFError>() {
                            let e = *e;
                            let message = e.to_string();
                            match e {
                                DFCatalogExternalDFError::OrdinalPositionParamOverflow {
                                    error,
                                    ..
                                } => {
                                    return Self {
                                        message: error.to_string(),
                                    };
                                }
                                DFCatalogExternalDFError::RidParamDoesntFitInU8 {
                                    error, ..
                                } => {
                                    return Self {
                                        message: error.to_string(),
                                    };
                                }
                                DFCatalogExternalDFError::CoreHistory { error, .. } => {
                                    return Self {
                                        message: error.to_string(),
                                    };
                                }
                                DFCatalogExternalDFError::CoreUtils { error, .. } => {
                                    return Self {
                                        message: error.to_string(),
                                    };
                                }
                                DFCatalogExternalDFError::CatalogNotFound { .. } => {
                                    return Self { message };
                                }
                                DFCatalogExternalDFError::ObjectStoreNotFound { .. } => {
                                    return Self { message };
                                }
                                _ => return Self { message },
                            }
                        } else {
                            unreachable!()
                        }
                    } else {
                        Self {
                            message: format!("Can't downcast error: {err}"),
                        }
                    }
                }
                _ => Self {
                    message: error.to_string(),
                },
            },
            Error::Metastore { source, .. } => {
                let source = *source;
                match source {
                    MetastoreError::TableDataExists { .. } => Self { message },
                    MetastoreError::TableRequirementFailed { .. } => Self { message },
                    MetastoreError::VolumeValidationFailed { .. } => Self { message },
                    MetastoreError::VolumeMissingCredentials { .. } => Self { message },
                    MetastoreError::CloudProviderNotImplemented { .. } => Self { message },
                    MetastoreError::ObjectStore { .. } => Self { message },
                    MetastoreError::ObjectStorePath { .. } => Self { message },
                    MetastoreError::CreateDirectory { .. } => Self { message },
                    MetastoreError::SlateDB { .. } => Self { message },
                    MetastoreError::UtilSlateDB { .. } => Self { message },
                    MetastoreError::ObjectAlreadyExists { .. } => Self { message },
                    MetastoreError::ObjectNotFound { .. } => Self { message },
                    MetastoreError::VolumeAlreadyExists { .. } => Self { message },
                    MetastoreError::VolumeNotFound { .. } => Self { message },
                    MetastoreError::DatabaseAlreadyExists { .. } => Self { message },
                    MetastoreError::DatabaseNotFound { .. } => Self { message },
                    MetastoreError::SchemaAlreadyExists { .. } => Self { message },
                    MetastoreError::SchemaNotFound { .. } => Self { message },
                    MetastoreError::TableAlreadyExists { .. } => Self { message },
                    MetastoreError::TableNotFound { .. } => Self { message },
                    MetastoreError::TableObjectStoreNotFound { .. } => Self { message },
                    MetastoreError::VolumeInUse { .. } => Self { message },
                    MetastoreError::Iceberg { .. } => Self { message },
                    MetastoreError::TableMetadataBuilder { .. } => Self { message },
                    MetastoreError::Serde { .. } => Self { message },
                    MetastoreError::Validation { .. } => Self { message },
                    MetastoreError::UrlParse { .. } => Self { message },
                    _ => Self { message },
                }
            }
            Error::InvalidTableIdentifier { .. } => Self { message },
            Error::InvalidSchemaIdentifier { .. } => Self { message },
            Error::InvalidFilePath { .. } => Self { message },
            Error::InvalidBucketIdentifier { .. } => Self { message },
            Error::Arrow { .. } => Self { message },
            Error::TableProviderNotFound { .. } => Self { message },
            Error::MissingDataFusionSession { .. } => Self { message },
            Error::ObjectAlreadyExists { .. } => Self { message },
            Error::UnsupportedFileFormat { .. } => Self { message },
            Error::RefreshCatalogList { .. } => Self { message },
            Error::CatalogDownCast { .. } => Self { message },
            Error::CatalogNotFound { .. } => Self { message },
            Error::S3Tables { .. } => Self { message },
            Error::ObjectStore { .. } => Self { message },
            Error::Utf8 { .. } => Self { message },
            Error::DatabaseNotFound { .. } => Self { message },
            Error::TableNotFound { .. } => Self { message },
            Error::SchemaNotFound { .. } => Self { message },
            Error::VolumeNotFound { .. } => Self { message },
            Error::Iceberg { .. } => Self { message },
            Error::UrlParse { .. } => Self { message },
            Error::JobError { .. } => Self { message },
            Error::UploadFailed { .. } => Self { message },
            Error::CatalogListDowncast { .. } => Self { message },
            Error::RegisterCatalog { .. } => Self { message },
            Error::SerdeParse { .. } => Self { message },
            Error::OnyUseWithVariables { .. } => Self { message },
            Error::OnlyPrimitiveStatements { .. } => Self { message },
            Error::OnlyTableSchemaCreateStatements { .. } => Self { message },
            Error::OnlyDropStatements { .. } => Self { message },
            Error::OnlyDropTableViewStatements { .. } => Self { message },
            Error::OnlyCreateTableStatements { .. } => Self { message },
            Error::OnlyCreateStageStatements { .. } => Self { message },
            Error::OnlyCopyIntoStatements { .. } => Self { message },
            Error::FromObjectRequiredForCopyIntoStatements { .. } => Self { message },
            Error::OnlyMergeStatements { .. } => Self { message },
            Error::OnlyCreateSchemaStatements { .. } => Self { message },
            Error::OnlySimpleSchemaNames { .. } => Self { message },
            Error::UnsupportedShowStatement { .. } => Self { message },
            Error::NoTableNamesForTruncateTable { .. } => Self { message },
            Error::OnlySQLStatements { .. } => Self { message },
            Error::MissingOrInvalidColumn { .. } => Self { message },
            Error::UnimplementedFunction { .. } => Self { message },
            Error::SqlParser { .. } => Self { message },
            _ => Self { message }, // unhandled errors
        }
    }
}
