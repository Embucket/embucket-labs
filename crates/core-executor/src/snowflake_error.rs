#![allow(clippy::redundant_else)]
#![allow(clippy::match_same_arms)]
use crate::error::Error;
use core_metastore::error::Error as MetastoreError;
use core_utils::errors::Error as DbError;
use datafusion::arrow::error::ArrowError;
use datafusion_common::Diagnostic;
use datafusion_common::diagnostic::DiagnosticKind;
use datafusion_common::error::DataFusionError;
use df_catalog::df_error::DFExternalError as DFCatalogExternalDFError;
use df_catalog::error::Error as CatalogError;
use embucket_functions::df_error::DFExternalError as EmubucketFunctionsExternalDFError;
use iceberg_rust::error::Error as IcebergError;
use slatedb::SlateDBError;
use snafu::GenerateImplicitData;
use snafu::{Location, Snafu, location};
use sqlparser::parser::ParserError;

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
    CompilationUnsupportedFeature {
        error: String,
        #[snafu(implicit)]
        location: Location,
    },

    // Verified: this Diagnostic error has span
    #[snafu(display("{} line {} at position {}\n{}",
        if error.kind == DiagnosticKind::Error { "error" } else { "warning" },
        if let Some(span) = error.span { span.start.line } else { 0 },
        if let Some(span) = error.span { span.start.column } else { 0 },
        error.message,
    ))]
    CompilationDiagnosticGeneric {
        error: Diagnostic,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{}", error.message))]
    CompilationDiagnosticEmptySpan {
        error: Diagnostic,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{error}"))]
    CompilationUnknown {
        error: String,
        #[snafu(implicit)]
        location: Location,
    },
}

impl SnowflakeError {
    #[must_use]
    pub fn display_debug_error_messages(&self) -> (String, String) {
        (self.to_string(), format!("{self:?}"))
    }
}

// Self { message: format!("SQL execution error: {}", message) }
impl SnowflakeError {
    pub fn from_executor_error(value: &Error) -> Self {
        let message = value.to_string();
        match value {
            Error::RegisterUDF { error, .. }
            | Error::RegisterUDAF { error, .. }
            | Error::DataFusionQuery { error, .. }
            | Error::DataFusionLogicalPlanMergeTarget { error, .. }
            | Error::DataFusionLogicalPlanMergeSource { error, .. }
            | Error::DataFusionLogicalPlanMergeJoin { error, .. }
            | Error::DataFusion { error, .. } => datafusion_error(error, &[]),
            Error::Metastore { source, .. } => metastore_error(source, &[]),
            Error::Iceberg { error, .. } => iceberg_error(error, &[]),
            Error::RefreshCatalogList { source, .. }
            | Error::RegisterCatalog { source, .. }
            | Error::DropDatabase { source, .. }
            | Error::CreateDatabase { source, .. } => catalog_error(source, &[]),
            _ => CustomSnafu { message }.build(),
        }
    }
}

fn format_message(subtext: &[&str], error: String) -> String {
    let subtext = subtext
        .iter()
        .filter(|s| !s.is_empty())
        .copied()
        .collect::<Vec<_>>()
        .join(" ");
    if subtext.is_empty() {
        error
    } else {
        format!("{subtext}: {error}")
    }
}

fn catalog_error(error: &CatalogError, subtext: &[&str]) -> SnowflakeError {
    let subtext = [subtext, &["Catalog"]].concat();
    match error {
        CatalogError::Metastore { source, .. } => metastore_error(source, &subtext),
        _ => CustomSnafu {
            message: format_message(&subtext, error.to_string()),
        }
        .build(),
    }
}

fn core_utils_error(error: &core_utils::Error, subtext: &[&str]) -> SnowflakeError {
    let subtext = [subtext, &["Db"]].concat();
    match error {
        DbError::Database { error, .. }
        | DbError::KeyGet { error, .. }
        | DbError::KeyDelete { error, .. }
        | DbError::KeyPut { error, .. }
        | DbError::ScanFailed { error, .. } => match error {
            SlateDBError::ObjectStoreError(obj_store_error) => {
                object_store_error(obj_store_error, &subtext)
            }
            _ => CustomSnafu {
                message: format_message(&subtext, error.to_string()),
            }
            .build(),
        },
        _ => CustomSnafu {
            message: format_message(&subtext, error.to_string()),
        }
        .build(),
    }
}

fn metastore_error(error: &MetastoreError, subtext: &[&str]) -> SnowflakeError {
    let subtext = [subtext, &["Metastore"]].concat();
    let message = error.to_string();
    match error {
        MetastoreError::ObjectStore { error, .. } => object_store_error(error, &subtext),
        MetastoreError::UtilSlateDB { source, .. } => core_utils_error(source, &subtext),
        _ => CustomSnafu {
            message: format_message(&subtext, message),
        }
        .build(),
    }
}

fn object_store_error(error: &object_store::Error, subtext: &[&str]) -> SnowflakeError {
    let subtext = [subtext, &["Object store"]].concat();
    CustomSnafu {
        message: format_message(&subtext, error.to_string()),
    }
    .build()
}

fn iceberg_error(error: &IcebergError, subtext: &[&str]) -> SnowflakeError {
    let subtext = [subtext, &["Iceberg"]].concat();
    match error {
        IcebergError::ObjectStore(error) => object_store_error(error, &subtext),
        IcebergError::External(err) => {
            if let Some(e) = err.downcast_ref::<MetastoreError>() {
                metastore_error(e, &subtext)
            } else if let Some(e) = err.downcast_ref::<object_store::Error>() {
                object_store_error(e, &subtext)
            } else {
                // Accidently CustomSnafu can't see internal field, so create error manually!
                SnowflakeError::Custom {
                    message: err.to_string(),
                    // Add downcast warning separately as this is internal message
                    internal: InternalMessage(format!("Warning: Didn't downcast error: {err}")),
                    location: location!(),
                }
            }
        }
        IcebergError::NotFound(message) => CustomSnafu { message }.build(),
        _ => CustomSnafu {
            message: format_message(&subtext, error.to_string()),
        }
        .build(),
    }
}

#[allow(clippy::too_many_lines)]
fn datafusion_error(df_error: &DataFusionError, subtext: &[&str]) -> SnowflakeError {
    let subtext = [subtext, &["DataFusion"]].concat();
    let message = df_error.to_string();
    match df_error {
        DataFusionError::ArrowError(arrow_error, ..) => {
            match arrow_error {
                ArrowError::ParquetError(message) => CustomSnafu { message }.build(),
                ArrowError::ExternalError(err) => {
                    // Accidently CustomSnafu can't see internal field, so create error manually!
                    SnowflakeError::Custom {
                        message: err.to_string(),
                        // Add downcast warning separately as this is internal message
                        internal: InternalMessage(format!("Warning: Didn't downcast error: {err}")),
                        location: location!(),
                    }
                }
                _ => CustomSnafu { message }.build(),
            }
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
            let diagnostic = *diagnostic.clone();
            // TODO: Should we use Plan error somehow?
            // two errors provided: what if it contains some additional data and not just message copy?
            // Following goes here:
            // SQL compilation error: Object 'DATABASE.PUBLIC.ARRAY_DATA' does not exist or not authorized.
            let diagn_error = if diagnostic.span.is_some() {
                CompilationDiagnosticGenericSnafu { error: diagnostic }.build()
            } else {
                CompilationDiagnosticEmptySpanSnafu { error: diagnostic }.build()
            };
            SnowflakeError::SqlCompilation { error: diagn_error }
        }
        DataFusionError::Execution(error) => SnowflakeError::SqlCompilation {
            error: CompilationUnknownSnafu { error }.build(),
        },
        DataFusionError::IoError(_io_error) => CustomSnafu { message }.build(),
        // Not implemented is just a string, no structured error data.
        // no feature name, no parser data: line, column
        DataFusionError::NotImplemented(error) => SnowflakeError::SqlCompilation {
            error: CompilationUnsupportedFeatureSnafu { error }.build(),
        },
        DataFusionError::ObjectStore(_object_store_error) => CustomSnafu { message }.build(),
        DataFusionError::ParquetError(_parquet_error) => CustomSnafu { message }.build(),
        DataFusionError::SchemaError(_schema_error, _boxed_backtrace) => {
            CustomSnafu { message }.build()
        }
        DataFusionError::Shared(_shared_error) => CustomSnafu { message }.build(),
        DataFusionError::SQL(sql_error, Some(_backtrace)) => match sql_error {
            ParserError::TokenizerError(error) | ParserError::ParserError(error) =>
            // Can produce message like this: "syntax error line 1 at position 27 unexpected 'XXXX'"
            // since parse error is just a text and not a structure
            {
                SnowflakeError::SqlCompilation {
                    error: CompilationUnknownSnafu { error }.build(),
                }
            }
            ParserError::RecursionLimitExceeded => CustomSnafu { message }.build(),
        },
        DataFusionError::ExecutionJoin(join_error) => CustomSnafu {
            message: join_error.to_string(),
        }
        .build(),
        DataFusionError::Substrait(_substrait_error) => CustomSnafu { message }.build(),
        DataFusionError::Internal(_internal_error) => CustomSnafu { message }.build(),
        DataFusionError::External(err) => {
            if let Some(e) = err.downcast_ref::<DataFusionError>() {
                datafusion_error(e, &subtext)
            } else if let Some(e) = err.downcast_ref::<Error>() {
                CustomSnafu {
                    message: e.to_string(),
                }
                .build()
            } else if let Some(e) = err.downcast_ref::<object_store::Error>() {
                CustomSnafu {
                    message: e.to_string(),
                }
                .build()
            } else if let Some(e) = err.downcast_ref::<iceberg_rust::error::Error>() {
                iceberg_error(e, &subtext)
            } else if let Some(e) = err.downcast_ref::<DbError>() {
                core_utils_error(e, &subtext)
            } else if let Some(e) = err.downcast_ref::<EmubucketFunctionsExternalDFError>() {
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
                    EmubucketFunctionsExternalDFError::Regexp { .. } => {
                        CustomSnafu { message }.build()
                    }
                }
            } else if let Some(e) = err.downcast_ref::<DFCatalogExternalDFError>() {
                let message = e.to_string();
                match e {
                    DFCatalogExternalDFError::OrdinalPositionParamOverflow { .. } => {
                        CustomSnafu { message }.build()
                    }
                    DFCatalogExternalDFError::RidParamDoesntFitInU8 { .. } => {
                        CustomSnafu { message }.build()
                    }
                    DFCatalogExternalDFError::CoreHistory { .. } => CustomSnafu { message }.build(),
                    DFCatalogExternalDFError::CoreUtils { .. } => CustomSnafu { message }.build(),
                    DFCatalogExternalDFError::CatalogNotFound { .. } => {
                        CustomSnafu { message }.build()
                    }
                    DFCatalogExternalDFError::ObjectStoreNotFound { .. } => {
                        CustomSnafu { message }.build()
                    }
                }
            } else if let Some(e) = err.downcast_ref::<ArrowError>() {
                CustomSnafu {
                    message: e.to_string(),
                }
                .build()
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
