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
use snafu::GenerateImplicitData;
use snafu::{Location, Snafu, location};
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
    Custom {
        message: String,
        #[snafu(implicit)]
        internal: InternalMessage,
        #[snafu(implicit)]
        location: Location,
    },
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct InternalMessage(String);

impl GenerateImplicitData for InternalMessage {
    #[inline]
    #[track_caller]
    fn generate() -> Self {
        Self(String::new())
    }
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
            | Error::DataFusion { error, .. } => datafusion_error(*error),
            Error::Metastore { source, .. } => {
                let source = *source;
                match source {
                    MetastoreError::TableDataExists { .. } => CustomSnafu { message }.build(),
                    MetastoreError::TableRequirementFailed { .. } => {
                        CustomSnafu { message }.build()
                    }
                    MetastoreError::VolumeValidationFailed { .. } => {
                        CustomSnafu { message }.build()
                    }
                    MetastoreError::VolumeMissingCredentials { .. } => {
                        CustomSnafu { message }.build()
                    }
                    MetastoreError::CloudProviderNotImplemented { .. } => {
                        CustomSnafu { message }.build()
                    }
                    MetastoreError::ObjectStore { .. } => CustomSnafu { message }.build(),
                    MetastoreError::ObjectStorePath { .. } => CustomSnafu { message }.build(),
                    MetastoreError::CreateDirectory { .. } => CustomSnafu { message }.build(),
                    MetastoreError::SlateDB { .. } => CustomSnafu { message }.build(),
                    MetastoreError::UtilSlateDB { .. } => CustomSnafu { message }.build(),
                    MetastoreError::ObjectAlreadyExists { .. } => CustomSnafu { message }.build(),
                    MetastoreError::ObjectNotFound { .. } => CustomSnafu { message }.build(),
                    MetastoreError::VolumeAlreadyExists { .. } => CustomSnafu { message }.build(),
                    MetastoreError::VolumeNotFound { .. } => CustomSnafu { message }.build(),
                    MetastoreError::DatabaseAlreadyExists { .. } => CustomSnafu { message }.build(),
                    MetastoreError::DatabaseNotFound { .. } => CustomSnafu { message }.build(),
                    MetastoreError::SchemaAlreadyExists { .. } => CustomSnafu { message }.build(),
                    MetastoreError::SchemaNotFound { .. } => CustomSnafu { message }.build(),
                    MetastoreError::TableAlreadyExists { .. } => CustomSnafu { message }.build(),
                    MetastoreError::TableNotFound { .. } => CustomSnafu { message }.build(),
                    MetastoreError::TableObjectStoreNotFound { .. } => {
                        CustomSnafu { message }.build()
                    }
                    MetastoreError::VolumeInUse { .. } => CustomSnafu { message }.build(),
                    MetastoreError::DatabaseInUse { .. } => CustomSnafu { message }.build(),
                    MetastoreError::Iceberg { .. } => CustomSnafu { message }.build(),
                    MetastoreError::TableMetadataBuilder { .. } => CustomSnafu { message }.build(),
                    MetastoreError::Serde { .. } => CustomSnafu { message }.build(),
                    MetastoreError::Validation { .. } => CustomSnafu { message }.build(),
                    MetastoreError::UrlParse { .. } => CustomSnafu { message }.build(),
                }
            }
            Error::InvalidDatabaseIdentifier { .. } => CustomSnafu { message }.build(),
            Error::InvalidTableIdentifier { .. } => CustomSnafu { message }.build(),
            Error::InvalidSchemaIdentifier { .. } => CustomSnafu { message }.build(),
            Error::DropDatabase { .. } => CustomSnafu { message }.build(),
            Error::CreateDatabase { .. } => CustomSnafu { message }.build(),
            Error::InvalidFilePath { .. } => CustomSnafu { message }.build(),
            Error::InvalidBucketIdentifier { .. } => CustomSnafu { message }.build(),
            Error::Arrow { .. } => CustomSnafu { message }.build(),
            Error::TableProviderNotFound { .. } => CustomSnafu { message }.build(),
            Error::MissingDataFusionSession { .. } => CustomSnafu { message }.build(),
            Error::ObjectAlreadyExists { .. } => CustomSnafu { message }.build(),
            Error::UnsupportedFileFormat { .. } => CustomSnafu { message }.build(),
            Error::RefreshCatalogList { .. } => CustomSnafu { message }.build(),
            Error::CatalogDownCast { .. } => CustomSnafu { message }.build(),
            Error::CatalogNotFound { .. } => CustomSnafu { message }.build(),
            Error::S3Tables { .. } => CustomSnafu { message }.build(),
            Error::ObjectStore { .. } => CustomSnafu { message }.build(),
            Error::Utf8 { .. } => CustomSnafu { message }.build(),
            Error::DatabaseNotFound { .. } => CustomSnafu { message }.build(),
            Error::TableNotFound { .. } => CustomSnafu { message }.build(),
            Error::SchemaNotFound { .. } => CustomSnafu { message }.build(),
            Error::VolumeNotFound { .. } => CustomSnafu { message }.build(),
            Error::Iceberg { .. } => CustomSnafu { message }.build(),
            Error::UrlParse { .. } => CustomSnafu { message }.build(),
            Error::JobError { .. } => CustomSnafu { message }.build(),
            Error::UploadFailed { .. } => CustomSnafu { message }.build(),
            Error::CatalogListDowncast { .. } => CustomSnafu { message }.build(),
            Error::RegisterCatalog { .. } => CustomSnafu { message }.build(),
            Error::ExternalVolumeRequiredForCreateDatabase { .. } => {
                CustomSnafu { message }.build()
            }
            Error::SerdeParse { .. } => CustomSnafu { message }.build(),
            Error::OnyUseWithVariables { .. } => CustomSnafu { message }.build(),
            Error::OnlyPrimitiveStatements { .. } => CustomSnafu { message }.build(),
            Error::OnlyDropStatements { .. } => CustomSnafu { message }.build(),
            Error::OnlyDropTableViewStatements { .. } => CustomSnafu { message }.build(),
            Error::OnlyCreateTableStatements { .. } => CustomSnafu { message }.build(),
            Error::OnlyCreateStageStatements { .. } => CustomSnafu { message }.build(),
            Error::OnlyCopyIntoStatements { .. } => CustomSnafu { message }.build(),
            Error::FromObjectRequiredForCopyIntoStatements { .. } => {
                CustomSnafu { message }.build()
            }
            Error::OnlyMergeStatements { .. } => CustomSnafu { message }.build(),
            Error::OnlyCreateSchemaStatements { .. } => CustomSnafu { message }.build(),
            Error::OnlyCreateViewStatements { .. } => CustomSnafu { message }.build(),
            Error::OnlySimpleSchemaNames { .. } => CustomSnafu { message }.build(),
            Error::UnsupportedShowStatement { .. } => CustomSnafu { message }.build(),
            Error::NoTableNamesForTruncateTable { .. } => CustomSnafu { message }.build(),
            Error::OnlySQLStatements { .. } => CustomSnafu { message }.build(),
            Error::MissingOrInvalidColumn { .. } => CustomSnafu { message }.build(),
            Error::UnimplementedFunction { .. } => CustomSnafu { message }.build(),
            Error::SqlParser { .. } => CustomSnafu { message }.build(),
            Error::InvalidColumnIdentifier { .. } => CustomSnafu { message }.build(),
            Error::NotMatchedBySourceNotSupported { .. } => CustomSnafu { message }.build(),
            Error::MergeInsertOnlyOneRow { .. } => CustomSnafu { message }.build(),
            Error::MergeTargetMustBeTable { .. } => CustomSnafu { message }.build(),
            Error::MergeSourceNotSupported { .. } => CustomSnafu { message }.build(),
            Error::MergeTargetMustBeIcebergTable { .. } => CustomSnafu { message }.build(),
            Error::LogicalExtensionChildCount { .. } => CustomSnafu { message }.build(),
            Error::MergeFilterStreamNotMatching { .. } => CustomSnafu { message }.build(),
            Error::MatchingFilesAlreadyConsumed { .. } => CustomSnafu { message }.build(),
            Error::MissingFilterPredicates { .. } => CustomSnafu { message }.build(),
            Error::UnsupportedIcebergValueType { .. } => CustomSnafu { message }.build(),
        }
    }
}

#[allow(clippy::too_many_lines)]
fn datafusion_error(df_error: DataFusionError) -> SnowflakeError {
    let message = df_error.to_string();
    match df_error {
        DataFusionError::ArrowError(_arrow_error, Some(_backtrace)) => {
            CustomSnafu { message }.build()
        }
        DataFusionError::Plan(_err) => CustomSnafu { message }.build(),
        DataFusionError::Collection(_df_errors) => {
            // In cases where we can return Collection of errors, we can have the most extended error context.
            // For instance it could include some DataFusionError provided as is, and External error encoding
            // any information we want.
            CustomSnafu { message }.build()
        }
        DataFusionError::Context(_context, _inner) => CustomSnafu { message }.build(),
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
        DataFusionError::IoError(_io_error) => CustomSnafu { message }.build(),
        // Not implemented is just a string, no structured error data.
        // no feature name, no parser data: line, column
        DataFusionError::NotImplemented(error) => SnowflakeError::SqlCompilation {
            error: SqlCompilationError::UnsupportedFeature { error },
        },
        DataFusionError::ObjectStore(_object_store_error) => CustomSnafu { message }.build(),
        DataFusionError::ParquetError(_parquet_error) => CustomSnafu { message }.build(),
        DataFusionError::SchemaError(_schema_error, _boxed_backtrace) => {
            CustomSnafu { message }.build()
        }
        DataFusionError::Shared(_shared_error) => CustomSnafu { message }.build(),
        DataFusionError::SQL(sql_error, Some(_backtrace)) => match sql_error {
            ParserError::TokenizerError(err) | ParserError::ParserError(err) =>
            // Can produce message like this: "syntax error line 1 at position 27 unexpected 'XXXX'"
            // since parse error is just a text and not a structure
            {
                SnowflakeError::SqlCompilation {
                    error: SqlCompilationError::Unknown { error: err },
                }
            }
            ParserError::RecursionLimitExceeded => CustomSnafu { message }.build(),
        },
        DataFusionError::Substrait(_substrait_error) => CustomSnafu { message }.build(),
        // DataFusionError::External(_external_error) => CustomSnafu { message }.build(),
        DataFusionError::Internal(_internal_error) => CustomSnafu { message }.build(),
        DataFusionError::External(err) => {
            if err.is::<DataFusionError>() {
                if let Ok(e) = err.downcast::<DataFusionError>() {
                    let err = *e;
                    datafusion_error(err)
                } else {
                    unreachable!()
                }
            } else if err.is::<Error>() {
                if let Ok(e) = err.downcast::<Error>() {
                    let e = *e;
                    let message = e.to_string();
                    CustomSnafu { message }.build()
                } else {
                    unreachable!()
                }
            } else if err.is::<object_store::Error>() {
                if let Ok(e) = err.downcast::<object_store::Error>() {
                    let e = *e;
                    let message = e.to_string();
                    CustomSnafu { message }.build()
                } else {
                    unreachable!()
                }
            } else if err.is::<iceberg_rust::error::Error>() {
                if let Ok(e) = err.downcast::<iceberg_rust::error::Error>() {
                    let e = *e;
                    let message = e.to_string();
                    match e {
                        iceberg_rust::error::Error::ObjectStore(e) => CustomSnafu {
                            message: format!("Object store error: {e}"),
                        }
                        .build(),
                        _ => CustomSnafu { message }.build(),
                    }
                } else {
                    unreachable!()
                }
            } else if err.is::<EmubucketFunctionsExternalDFError>() {
                if let Ok(e) = err.downcast::<EmubucketFunctionsExternalDFError>() {
                    let e = *e;
                    let message = e.to_string();
                    match e {
                        EmubucketFunctionsExternalDFError::Aggregate { .. } => {
                            CustomSnafu { message }.build()
                        }
                        EmubucketFunctionsExternalDFError::Conversion { .. } => {
                            CustomSnafu { message }.build()
                        }
                        EmubucketFunctionsExternalDFError::DateTime { .. } => {
                            CustomSnafu { message }.build()
                        }
                        EmubucketFunctionsExternalDFError::Numeric { .. } => {
                            CustomSnafu { message }.build()
                        }
                        EmubucketFunctionsExternalDFError::SemiStructured { .. } => {
                            CustomSnafu { message }.build()
                        }
                        EmubucketFunctionsExternalDFError::StringBinary { .. } => {
                            CustomSnafu { message }.build()
                        }
                        EmubucketFunctionsExternalDFError::Table { .. } => {
                            CustomSnafu { message }.build()
                        }
                        EmubucketFunctionsExternalDFError::Crate { .. } => {
                            CustomSnafu { message }.build()
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
                        DFCatalogExternalDFError::OrdinalPositionParamOverflow { .. } => {
                            CustomSnafu { message }.build()
                        }
                        DFCatalogExternalDFError::RidParamDoesntFitInU8 { .. } => {
                            CustomSnafu { message }.build()
                        }
                        DFCatalogExternalDFError::CoreHistory { .. } => {
                            CustomSnafu { message }.build()
                        }
                        DFCatalogExternalDFError::CoreUtils { .. } => {
                            CustomSnafu { message }.build()
                        }
                        DFCatalogExternalDFError::CatalogNotFound { .. } => {
                            CustomSnafu { message }.build()
                        }
                        DFCatalogExternalDFError::ObjectStoreNotFound { .. } => {
                            CustomSnafu { message }.build()
                        }
                    }
                } else {
                    unreachable!()
                }
            } else {
                // Accidently CustomSnafu can't see internal field, so create error manually!
                SnowflakeError::Custom {
                    message,
                    // Add downcast warning separately as this is internal message
                    internal: InternalMessage(format!("Warning: Didn't downcast error: {err}")),
                    location: location!(),
                }
            }
        }
        _ => CustomSnafu { message }.build(),
    }
}
