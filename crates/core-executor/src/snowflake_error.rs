#![allow(clippy::redundant_else)]
#![allow(clippy::match_same_arms)]
use crate::error::Error;
use core_metastore::error::Error as MetastoreError;
use datafusion_common::Diagnostic;
use datafusion_common::Span;
use datafusion_common::diagnostic::DiagnosticKind;
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
pub enum SnowflakeError {
    #[snafu(display("SQL compilation error: {error}"))]
    SqlCompilation { error: SqlCompilationError },
    #[snafu(display("{message}"))]
    Custom { message: String },
}

#[derive(Snafu, Debug)]
pub enum SqlCompilationError {
    #[snafu(display("unsupported feature: {error}"))]
    UnsupportedFeature { error: String },
    // Split Diagnostic on two errors: with span and without span
    #[snafu(display("{} line {} at position {}\n{}",
        if error.kind == DiagnosticKind::Error { "error" } else { "warning" },
        span.start.line, span.start.column,
        error.message,
    ))]
    DiagnosticWithSpan { span: Span, error: Diagnostic },
    #[snafu(display("{error:?}"))]
    DiagnosticGeneric { error: Diagnostic },
    #[snafu(display("{error}"))]
    Unknown { error: String },
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
                                    Self::Custom { message }
                                }
                                EmubucketFunctionsExternalDFError::Conversion { .. } => {
                                    Self::Custom { message }
                                }
                                EmubucketFunctionsExternalDFError::DateTime { .. } => {
                                    Self::Custom { message }
                                }
                                EmubucketFunctionsExternalDFError::Numeric { .. } => {
                                    Self::Custom { message }
                                }
                                EmubucketFunctionsExternalDFError::SemiStructured { .. } => {
                                    Self::Custom { message }
                                }
                                EmubucketFunctionsExternalDFError::StringBinary { .. } => {
                                    Self::Custom { message }
                                }
                                EmubucketFunctionsExternalDFError::Table { .. } => {
                                    Self::Custom { message }
                                }
                                EmubucketFunctionsExternalDFError::Crate { .. } => {
                                    Self::Custom { message }
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
                                } => Self::Custom { message },
                                DFCatalogExternalDFError::RidParamDoesntFitInU8 { .. } => {
                                    Self::Custom { message }
                                }
                                DFCatalogExternalDFError::CoreHistory { .. } => {
                                    Self::Custom { message }
                                }
                                DFCatalogExternalDFError::CoreUtils { .. } => {
                                    Self::Custom { message }
                                }
                                DFCatalogExternalDFError::CatalogNotFound { .. } => {
                                    Self::Custom { message }
                                }
                                DFCatalogExternalDFError::ObjectStoreNotFound { .. } => {
                                    Self::Custom { message }
                                }
                            }
                        } else {
                            unreachable!()
                        }
                    } else {
                        Self::Custom {
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
                    MetastoreError::TableDataExists { .. } => Self::Custom { message },
                    MetastoreError::TableRequirementFailed { .. } => Self::Custom { message },
                    MetastoreError::VolumeValidationFailed { .. } => Self::Custom { message },
                    MetastoreError::VolumeMissingCredentials { .. } => Self::Custom { message },
                    MetastoreError::CloudProviderNotImplemented { .. } => Self::Custom { message },
                    MetastoreError::ObjectStore { .. } => Self::Custom { message },
                    MetastoreError::ObjectStorePath { .. } => Self::Custom { message },
                    MetastoreError::CreateDirectory { .. } => Self::Custom { message },
                    MetastoreError::SlateDB { .. } => Self::Custom { message },
                    MetastoreError::UtilSlateDB { .. } => Self::Custom { message },
                    MetastoreError::ObjectAlreadyExists { .. } => Self::Custom { message },
                    MetastoreError::ObjectNotFound { .. } => Self::Custom { message },
                    MetastoreError::VolumeAlreadyExists { .. } => Self::Custom { message },
                    MetastoreError::VolumeNotFound { .. } => Self::Custom { message },
                    MetastoreError::DatabaseAlreadyExists { .. } => Self::Custom { message },
                    MetastoreError::DatabaseNotFound { .. } => Self::Custom { message },
                    MetastoreError::SchemaAlreadyExists { .. } => Self::Custom { message },
                    MetastoreError::SchemaNotFound { .. } => Self::Custom { message },
                    MetastoreError::TableAlreadyExists { .. } => Self::Custom { message },
                    MetastoreError::TableNotFound { .. } => Self::Custom { message },
                    MetastoreError::TableObjectStoreNotFound { .. } => Self::Custom { message },
                    MetastoreError::VolumeInUse { .. } => Self::Custom { message },
                    MetastoreError::Iceberg { .. } => Self::Custom { message },
                    MetastoreError::TableMetadataBuilder { .. } => Self::Custom { message },
                    MetastoreError::Serde { .. } => Self::Custom { message },
                    MetastoreError::Validation { .. } => Self::Custom { message },
                    MetastoreError::UrlParse { .. } => Self::Custom { message },
                }
            }
            Error::InvalidTableIdentifier { .. } => Self::Custom { message },
            Error::InvalidSchemaIdentifier { .. } => Self::Custom { message },
            Error::InvalidFilePath { .. } => Self::Custom { message },
            Error::InvalidBucketIdentifier { .. } => Self::Custom { message },
            Error::Arrow { .. } => Self::Custom { message },
            Error::TableProviderNotFound { .. } => Self::Custom { message },
            Error::MissingDataFusionSession { .. } => Self::Custom { message },
            Error::ObjectAlreadyExists { .. } => Self::Custom { message },
            Error::UnsupportedFileFormat { .. } => Self::Custom { message },
            Error::RefreshCatalogList { .. } => Self::Custom { message },
            Error::CatalogDownCast { .. } => Self::Custom { message },
            Error::CatalogNotFound { .. } => Self::Custom { message },
            Error::S3Tables { .. } => Self::Custom { message },
            Error::ObjectStore { .. } => Self::Custom { message },
            Error::Utf8 { .. } => Self::Custom { message },
            Error::DatabaseNotFound { .. } => Self::Custom { message },
            Error::TableNotFound { .. } => Self::Custom { message },
            Error::SchemaNotFound { .. } => Self::Custom { message },
            Error::VolumeNotFound { .. } => Self::Custom { message },
            Error::Iceberg { .. } => Self::Custom { message },
            Error::UrlParse { .. } => Self::Custom { message },
            Error::JobError { .. } => Self::Custom { message },
            Error::UploadFailed { .. } => Self::Custom { message },
            Error::CatalogListDowncast { .. } => Self::Custom { message },
            Error::RegisterCatalog { .. } => Self::Custom { message },
            Error::SerdeParse { .. } => Self::Custom { message },
            Error::OnyUseWithVariables { .. } => Self::Custom { message },
            Error::OnlyPrimitiveStatements { .. } => Self::Custom { message },
            Error::OnlyTableSchemaCreateStatements { .. } => Self::Custom { message },
            Error::OnlyDropStatements { .. } => Self::Custom { message },
            Error::OnlyDropTableViewStatements { .. } => Self::Custom { message },
            Error::OnlyCreateTableStatements { .. } => Self::Custom { message },
            Error::OnlyCreateStageStatements { .. } => Self::Custom { message },
            Error::OnlyCopyIntoStatements { .. } => Self::Custom { message },
            Error::FromObjectRequiredForCopyIntoStatements { .. } => Self::Custom { message },
            Error::OnlyMergeStatements { .. } => Self::Custom { message },
            Error::OnlyCreateSchemaStatements { .. } => Self::Custom { message },
            Error::OnlySimpleSchemaNames { .. } => Self::Custom { message },
            Error::UnsupportedShowStatement { .. } => Self::Custom { message },
            Error::NoTableNamesForTruncateTable { .. } => Self::Custom { message },
            Error::OnlySQLStatements { .. } => Self::Custom { message },
            Error::MissingOrInvalidColumn { .. } => Self::Custom { message },
            Error::UnimplementedFunction { .. } => Self::Custom { message },
            Error::SqlParser { .. } => Self::Custom { message },
            Error::InvalidColumnIdentifier { .. } => Self::Custom { message },
            Error::NotMatchedBySourceNotSupported { .. } => Self::Custom { message },
            Error::MergeInsertOnlyOneRow { .. } => Self::Custom { message },
            Error::MergeTargetMustBeTable { .. } => Self::Custom { message },
            Error::MergeSourceNotSupported { .. } => Self::Custom { message },
            Error::MergeTargetMustBeIcebergTable { .. } => Self::Custom { message },
            Error::LogicalExtensionChildCount { .. } => Self::Custom { message },
            Error::MergeFilterStreamNotMatching { .. } => Self::Custom { message },
            Error::MatchingFilesAlreadyConsumed { .. } => Self::Custom { message },
            Error::MissingFilterPredicates { .. } => Self::Custom { message },
            Error::UnsupportedIcebergValueType { .. } => Self::Custom { message },
        }
    }
}

fn datafusion_error(datafusion_error: DataFusionError) -> SnowflakeError {
    let message = datafusion_error.to_string();
    match datafusion_error {
        DataFusionError::ArrowError(_arrow_error, Some(_backtrace)) => {
            SnowflakeError::Custom { message }
        }
        DataFusionError::Plan(_err) => SnowflakeError::Custom { message },
        DataFusionError::Collection(_df_errors) => {
            // In cases where we can return Collection of errors, we can have the most extended error context.
            // For instance it could include some DataFusionError provided as is, and External error encoding
            // any information we want.
            SnowflakeError::Custom { message }
        }
        DataFusionError::Context(_context, _inner) => SnowflakeError::Custom { message },
        DataFusionError::Diagnostic(diagnostic, _inner) => {
            // TODO: Should we use Plan error somehow?
            // two errors provided: what if it contains some additional data and not just message copy?
            // Following goes here:
            // SQL compilation error: Object 'DATABASE.PUBLIC.ARRAY_DATA' does not exist or not authorized.
            let diagn_error = if let Some(span) = diagnostic.span {
                SqlCompilationError::DiagnosticWithSpan {
                    span,
                    error: *diagnostic,
                }
            } else {
                SqlCompilationError::DiagnosticGeneric { error: *diagnostic }
            };
            SnowflakeError::SqlCompilation { error: diagn_error }
        }
        DataFusionError::Execution(err) => SnowflakeError::SqlCompilation {
            error: SqlCompilationError::Unknown { error: err },
        },
        DataFusionError::IoError(_io_error) => SnowflakeError::Custom { message },
        // Not implemented is just a string, no structured error data.
        // no feature name, no parser data: line, column
        DataFusionError::NotImplemented(error) => SnowflakeError::SqlCompilation {
            error: SqlCompilationError::UnsupportedFeature { error },
        },
        DataFusionError::ObjectStore(_object_store_error) => SnowflakeError::Custom { message },
        DataFusionError::ParquetError(_parquet_error) => SnowflakeError::Custom { message },
        DataFusionError::SchemaError(_schema_error, _boxed_backtrace) => {
            SnowflakeError::Custom { message }
        }
        DataFusionError::Shared(_shared_error) => SnowflakeError::Custom { message },
        DataFusionError::SQL(sql_error, Some(_backtrace)) => match sql_error {
            ParserError::TokenizerError(err) | ParserError::ParserError(err) =>
            // Can produce message like this: "syntax error line 1 at position 27 unexpected 'XXXX'"
            // since parse error is just a text and not a structure
            {
                SnowflakeError::SqlCompilation {
                    error: SqlCompilationError::Unknown { error: err },
                }
            }
            ParserError::RecursionLimitExceeded => SnowflakeError::Custom { message },
        },
        DataFusionError::Substrait(_substrait_error) => SnowflakeError::Custom { message },
        DataFusionError::External(_external_error) => SnowflakeError::Custom { message },
        DataFusionError::Internal(_internal_error) => SnowflakeError::Custom { message },
        _ => SnowflakeError::Custom { message },
    }
}
