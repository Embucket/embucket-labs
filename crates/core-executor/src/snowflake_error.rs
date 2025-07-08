#![allow(clippy::redundant_else)]
#![allow(clippy::match_same_arms)]
use crate::error::Error;
use core_metastore::error::Error as MetastoreError;
use datafusion_common::Diagnostic;
use datafusion_common::error::DataFusionError;
use df_catalog::df_error::DFExternalError as DFCatalogExternalDFError;
use embucket_functions::df_error::DFExternalError as EmubucketFunctionsExternalDFError;
use snafu::Snafu;
use sqlparser::parser::ParserError;

// How SLT tests are used in Snowflake error conversion?
// Database engine has a variety of errors, and you need to have error's structure to be able to match it
// and then properly return appropriate Snowflake error. Currently error_stack_trace available in logs and
// provides extended error context. It is helpful for cathing occasional errors. But with SLT tests
// you can get all the errors sources returned by Embucket, since slt runner produces slt_errors_stats_embucket.csv
// file having all the errors occured during a test run, including error_stack_trace column.

// 1. Use error_stack_trace to match error here inside `From<Error> for SnowflakeError`
// 2. When it comes to DataFusionError mostly it's not enough having a single error
// as couldn't have perserred location information, errorred entities names, etc.
// Use DataFusionError::Collection to return multiple errors providing additional context.
// 3. Cover custom format messages with tests in `tests/snowflake_errors.rs`

#[derive(Snafu, Debug)]
#[snafu(display("{message}"))]
pub struct SnowflakeError {
    pub message: String,
}

// Self { message: format!("SQL execution error: {}", message) }
impl From<Error> for SnowflakeError {
    #[allow(clippy::too_many_lines)]
    fn from(value: Error) -> Self {
        let message = value.to_string();
        match value {
            Error::RegisterUDF { error, .. }
            | Error::RegisterUDAF { error, .. }
            | Error::DataFusionQuery { error, .. }
            | Error::DataFusionLogicalPlanMergeTarget { error, .. }
            | Error::DataFusionLogicalPlanMergeSource { error, .. }
            | Error::DataFusionLogicalPlanMergeJoin { error, .. }
            | Error::DataFusion { error, .. } => match *error {
                DataFusionError::External(err) => {
                    if err.is::<DataFusionError>() {
                        if let Ok(e) = err.downcast::<DataFusionError>() {
                            let err = *e;
                            datafusion_error(err)
                        } else {
                            unreachable!()
                        }
                    } else if err.is::<EmubucketFunctionsExternalDFError>() {
                        if let Ok(e) = err.downcast::<EmubucketFunctionsExternalDFError>() {
                            let e = *e;
                            let message = e.to_string();
                            match e {
                                EmubucketFunctionsExternalDFError::Aggregate { .. } => {
                                    return Self { message };
                                }
                                EmubucketFunctionsExternalDFError::Conversion { .. } => {
                                    return Self { message };
                                }
                                EmubucketFunctionsExternalDFError::DateTime { .. } => {
                                    return Self { message };
                                }
                                EmubucketFunctionsExternalDFError::Numeric { .. } => {
                                    return Self { message };
                                }
                                EmubucketFunctionsExternalDFError::SemiStructured { .. } => {
                                    return Self { message };
                                }
                                EmubucketFunctionsExternalDFError::StringBinary { .. } => {
                                    return Self { message };
                                }
                                EmubucketFunctionsExternalDFError::Table { .. } => {
                                    return Self { message };
                                }
                                EmubucketFunctionsExternalDFError::Crate { .. } => {
                                    return Self { message };
                                }
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
                                    ..
                                } => {
                                    return Self { message };
                                }
                                DFCatalogExternalDFError::RidParamDoesntFitInU8 { .. } => {
                                    return Self { message };
                                }
                                DFCatalogExternalDFError::CoreHistory { .. } => {
                                    return Self { message };
                                }
                                DFCatalogExternalDFError::CoreUtils { .. } => {
                                    return Self { message };
                                }
                                DFCatalogExternalDFError::CatalogNotFound { .. } => {
                                    return Self { message };
                                }
                                DFCatalogExternalDFError::ObjectStoreNotFound { .. } => {
                                    return Self { message };
                                }
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
                // Rest of datafusion errors except External, which is handled above
                _ => datafusion_error(*error),
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
            Error::InvalidColumnIdentifier { .. } => Self { message },
            Error::NotMatchedBySourceNotSupported { .. } => Self { message },
            Error::MergeInsertOnlyOneRow { .. } => Self { message },
            Error::MergeTargetMustBeTable { .. } => Self { message },
            Error::MergeSourceNotSupported { .. } => Self { message },
            Error::MergeTargetMustBeIcebergTable { .. } => Self { message },
            Error::LogicalExtensionChildCount { .. } => Self { message },
            Error::MergeFilterStreamNotMatching { .. } => Self { message },
            Error::MatchingFilesAlreadyConsumed { .. } => Self { message },
            Error::MissingFilterPredicates { .. } => Self { message },
            Error::UnsupportedIcebergValueType { .. } => Self { message },
        }
    }
}

fn diagnostic_location_error(diagnostic: &Diagnostic) -> Option<String> {
    diagnostic.span.map(|span| {
        format!(
            "error line {} at position {}\n",
            span.start.line, span.start.column
        )
    })
}

fn datafusion_error(datafusion_error: DataFusionError) -> SnowflakeError {
    let message = datafusion_error.to_string();
    match datafusion_error {
        DataFusionError::ArrowError(_arrow_error, Some(_backtrace)) => SnowflakeError { message },
        DataFusionError::Plan(_err) => SnowflakeError { message },
        DataFusionError::Collection(_df_errors) => {
            // In cases where we can return Collection of errors, we can have the most extended error context.
            // For instance it could include some DataFusionError provided as is, and External error encoding
            // any information we want.
            SnowflakeError { message }
        }
        DataFusionError::Context(_context, _inner) => SnowflakeError { message },
        DataFusionError::Diagnostic(diagnostic, _inner) => {
            // TODO: Should we use Plan error somehow?
            // two errors provided: what if it contains some additional data and not just message copy?
            let diagnostic = *diagnostic;
            let location_error = diagnostic_location_error(&diagnostic).unwrap_or_default();
            SnowflakeError {
                // SQL compilation error: Object 'DATABASE.PUBLIC.ARRAY_DATA' does not exist or not authorized.
                // TODO: add line and column from diagnostic.span.
                message: format!(
                    "SQL compilation error: {location_error}{}",
                    diagnostic.message
                ),
            }
        }
        DataFusionError::Execution(err) => SnowflakeError {
            message: format!("SQL compilation error: {err}"),
        },
        DataFusionError::IoError(_io_error) => SnowflakeError { message },
        DataFusionError::NotImplemented(err) => {
            SnowflakeError {
                // Not implemented is just a string, no structured error data.
                // no feature name, no parser data: line, column
                message: format!("SQL compilation error: unsupported feature '{err}'"),
            }
        }
        DataFusionError::ObjectStore(_object_store_error) => SnowflakeError { message },
        DataFusionError::ParquetError(_parquet_error) => SnowflakeError { message },
        DataFusionError::SchemaError(_schema_error, _boxed_backtrace) => SnowflakeError { message },
        DataFusionError::Shared(_shared_error) => SnowflakeError { message },
        DataFusionError::SQL(sql_error, Some(_backtrace)) => match sql_error {
            ParserError::TokenizerError(err) | ParserError::ParserError(err) => SnowflakeError {
                // Can produce message like this: "syntax error line 1 at position 27 unexpected 'XXXX'"
                // since parse error is just a text and not a structure
                message: format!("SQL compilation error: {err}"),
            },
            ParserError::RecursionLimitExceeded => SnowflakeError { message },
        },
        DataFusionError::Substrait(_substrait_error) => SnowflakeError { message },
        DataFusionError::External(_external_error) => SnowflakeError { message },
        DataFusionError::Internal(_internal_error) => SnowflakeError { message },
        _ => SnowflakeError { message },
    }
}
