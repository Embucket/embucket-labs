// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::datatypes::{
    DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, DECIMAL128_MAX_PRECISION,
    DECIMAL256_MAX_PRECISION, DECIMAL_DEFAULT_SCALE,
};
use datafusion::common::{
    not_impl_err, plan_datafusion_err, plan_err, Constraints, DFSchema, ToDFSchema,
};
use datafusion::common::{DataFusionError, Result, SchemaError};
use datafusion::logical_expr::sqlparser::ast;
use datafusion::logical_expr::sqlparser::ast::{ArrayElemTypeDef, ColumnDef, ExactNumberInfo, Ident, StructBracketKind, TableConstraint};
use datafusion::logical_expr::{CreateMemoryTable, DdlStatement, EmptyRelation, LogicalPlan};
use datafusion::prelude::*;
use datafusion::sql::planner::{
    object_name_to_table_reference, ContextProvider, IdentNormalizer, PlannerContext, SqlToRel,
};
use datafusion::sql::sqlparser::ast::{
    ColumnDef as SQLColumnDef, ColumnOption, DataType as SQLDataType, Statement, TimezoneInfo,
};
use std::sync::Arc;

pub struct ExtendedSqlToRel<'a, S>
where
    S: ContextProvider,
{
    inner: SqlToRel<'a, S>, // The wrapped type
    provider: &'a S,
    normalizer: IdentNormalizer,
}

impl<'a, S> ExtendedSqlToRel<'a, S>
where
    S: ContextProvider,
{
    /// Create a new instance of `ExtendedSqlToRel`
    pub fn new(provider: &'a S) -> Self {
        Self {
            inner: SqlToRel::new(provider),
            provider,
            normalizer: Default::default(),
        }
    }

    /// Custom implementation of `sql_statement_to_plan`
    pub fn sql_statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        // Check for a custom statement type
        match self.handle_custom_statement(statement.clone()) {
            Ok(plan) => return Ok(plan),
            Err(e) => {
                eprintln!("Error: {e}");
            }
        }

        // For all other statements, delegate to the wrapped SqlToRel
        self.inner.sql_statement_to_plan(statement)
    }

    /// Handle custom statements not supported by the original `SqlToRel`
    fn handle_custom_statement(&self, statement: Statement) -> Result<LogicalPlan> {
        let planner_context: &mut PlannerContext = &mut PlannerContext::new();
        // Example: Custom handling for a specific statement
        match statement {
            // Statement::CreateTable {
            //     query,
            //     name,
            //     columns,
            //     constraints,
            //     table_properties,
            //     with_options,
            //     if_not_exists,
            //     or_replace,
            //     ..
            // } if table_properties.is_empty() && with_options.is_empty() => {
            //     // Merge inline constraints and existing constraints
            //     let mut all_constraints = constraints;
            //     let inline_constraints = calc_inline_constraints_from_columns(&columns);
            //     all_constraints.extend(inline_constraints);
            //     // Build column default values
            //     let column_defaults = self.build_column_defaults(&columns, planner_context)?;
            //     match query {
            //         None => {
            //             let schema = self.build_schema(columns)?.to_dfschema_ref()?;
            //             let plan = EmptyRelation {
            //                 produce_one_row: false,
            //                 schema,
            //             };
            //             let plan = LogicalPlan::EmptyRelation(plan);
            //             let constraints = Constraints::new_unverified(
            //                 &all_constraints,
            //             )?;
            //             Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
            //                 CreateMemoryTable {
            //                     name: object_name_to_table_reference(name, true)?,
            //                     constraints,
            //                     input: Arc::new(plan),
            //                     if_not_exists,
            //                     or_replace,
            //                     column_defaults,
            //                     temporary: false,
            //                 },
            //             )))
            //         }
            //         _ => Err(DataFusionError::NotImplemented(
            //             "CREATE TABLE with options is not supported bu custom implementation"
            //                 .to_string(),
            //         )),
            //     }
            // }
            _ => plan_err!("Unsupported statement: {:?}", statement),
        }
    }

    /// Returns a vector of (column_name, default_expr) pairs
    pub fn build_column_defaults(
        &self,
        columns: &Vec<SQLColumnDef>,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<(String, Expr)>> {
        let mut column_defaults = vec![];
        // Default expressions are restricted, column references are not allowed
        let empty_schema = DFSchema::empty();
        let error_desc = |e: DataFusionError| match e {
            DataFusionError::SchemaError(SchemaError::FieldNotFound { .. }, _) => {
                plan_datafusion_err!(
                    "Column reference is not allowed in the DEFAULT expression : {}",
                    e
                )
            }
            _ => e,
        };

        for column in columns {
            if let Some(default_sql_expr) = column.options.iter().find_map(|o| match &o.option {
                ColumnOption::Default(expr) => Some(expr),
                _ => None,
            }) {
                let default_expr = self
                    .inner
                    .sql_to_expr(default_sql_expr.clone(), &empty_schema, planner_context)
                    .map_err(error_desc)?;
                column_defaults
                    .push((self.normalizer.normalize(column.name.clone()), default_expr));
            }
        }
        Ok(column_defaults)
    }

    pub fn build_schema(&self, columns: Vec<SQLColumnDef>) -> Result<Schema> {
        let mut fields = Vec::with_capacity(columns.len());

        for column in columns {
            let data_type = self.convert_data_type(&column.data_type)?;
            let not_nullable = column
                .options
                .iter()
                .any(|x| x.option == ColumnOption::NotNull);
            fields.push(Field::new(
                self.normalizer.normalize(column.name),
                data_type,
                !not_nullable,
            ));
        }

        Ok(Schema::new(fields))
    }

    pub fn convert_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::Array(ArrayElemTypeDef::AngleBracket(inner_sql_type))
            | SQLDataType::Array(ArrayElemTypeDef::SquareBracket(inner_sql_type, _)) => {
                // Arrays may be multi-dimensional.
                let inner_data_type = self.convert_data_type(inner_sql_type)?;
                Ok(DataType::new_list(inner_data_type, true))
            }
            SQLDataType::Array(ArrayElemTypeDef::None) => {
                not_impl_err!("Arrays with unspecified type is not supported")
            }
            other => self.convert_simple_data_type(other),
        }
    }

    fn convert_simple_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::Boolean | SQLDataType::Bool => Ok(DataType::Boolean),
            SQLDataType::TinyInt(_) => Ok(DataType::Int8),
            SQLDataType::SmallInt(_) | SQLDataType::Int2(_) => Ok(DataType::Int16),
            SQLDataType::Int(_) | SQLDataType::Integer(_) | SQLDataType::Int4(_) => Ok(DataType::Int32),
            SQLDataType::BigInt(_) | SQLDataType::Int8(_) => Ok(DataType::Int64),
            SQLDataType::UnsignedTinyInt(_) => Ok(DataType::UInt8),
            SQLDataType::UnsignedSmallInt(_) | SQLDataType::UnsignedInt2(_) => Ok(DataType::UInt16),
            SQLDataType::UnsignedInt(_) | SQLDataType::UnsignedInteger(_) | SQLDataType::UnsignedInt4(_) => {
                Ok(DataType::UInt32)
            }
            SQLDataType::Varchar(length) => {
                match (length, true) {
                    (Some(_), false) => plan_err!("does not support Varchar with length, please set `support_varchar_with_length` to be true"),
                    _ => Ok(DataType::Utf8),
                }
            }
            SQLDataType::UnsignedBigInt(_) | SQLDataType::UnsignedInt8(_) => Ok(DataType::UInt64),
            SQLDataType::Float(_) => Ok(DataType::Float32),
            SQLDataType::Real | SQLDataType::Float4 => Ok(DataType::Float32),
            SQLDataType::Double | SQLDataType::DoublePrecision | SQLDataType::Float8 => Ok(DataType::Float64),
            SQLDataType::Char(_)
            | SQLDataType::Text
            | SQLDataType::String(_) => Ok(DataType::Utf8),
            SQLDataType::Timestamp(None, tz_info) => {
                let tz = if matches!(tz_info, TimezoneInfo::Tz)
                    || matches!(tz_info, TimezoneInfo::WithTimeZone)
                {
                    // Timestamp With Time Zone
                    // INPUT : [SQLDataType]   TimestampTz + [RuntimeConfig] Time Zone
                    // OUTPUT: [ArrowDataType] Timestamp<TimeUnit, Some(Time Zone)>
                    self.provider.options().execution.time_zone.clone()
                } else {
                    // Timestamp Without Time zone
                    None
                };
                Ok(DataType::Timestamp(TimeUnit::Nanosecond, tz.map(Into::into)))
            }
            SQLDataType::Date => Ok(DataType::Date32),
            SQLDataType::Time(None, tz_info) => {
                if matches!(tz_info, TimezoneInfo::None)
                    || matches!(tz_info, TimezoneInfo::WithoutTimeZone)
                {
                    Ok(DataType::Time64(TimeUnit::Nanosecond))
                } else {
                    // We dont support TIMETZ and TIME WITH TIME ZONE for now
                    not_impl_err!(
                        "Unsupported SQL type {sql_type:?}"
                    )
                }
            }
            SQLDataType::Numeric(exact_number_info)
            | SQLDataType::Decimal(exact_number_info) => {
                let (precision, scale) = match *exact_number_info {
                    ExactNumberInfo::None => (None, None),
                    ExactNumberInfo::Precision(precision) => (Some(precision), None),
                    ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                        (Some(precision), Some(scale))
                    }
                };
                make_decimal_type(precision, scale)
            }
            SQLDataType::Bytea => Ok(DataType::Binary),
            SQLDataType::Interval => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
            SQLDataType::Struct(fields, _) => {
                let fields = fields
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| {
                        let data_type = self.convert_data_type(&field.field_type)?;
                        let field_name = match &field.field_name {
                            Some(ident) => ident.clone(),
                            None => Ident::new(format!("c{idx}"))
                        };
                        Ok(Arc::new(Field::new(
                            self.normalizer.normalize(field_name),
                            data_type,
                            true,
                        )))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataType::Struct(Fields::from(fields)))
            }
            // https://github.com/apache/datafusion/issues/12644
            SQLDataType::JSON => Ok(DataType::Utf8),
            // Explicitly list all other types so that if sqlparser
            // adds/changes the `SQLDataType` the compiler will tell us on upgrade
            // and avoid bugs like https://github.com/apache/datafusion/issues/3059
            SQLDataType::Nvarchar(_)
            | SQLDataType::Uuid
            | SQLDataType::Binary(_)
            | SQLDataType::Varbinary(_)
            | SQLDataType::Blob(_)
            | SQLDataType::Datetime(_)
            | SQLDataType::Regclass
            | SQLDataType::Custom(_, _)
            | SQLDataType::Array(_)
            | SQLDataType::Enum(_)
            | SQLDataType::Set(_)
            | SQLDataType::MediumInt(_)
            | SQLDataType::UnsignedMediumInt(_)
            | SQLDataType::Character(_)
            | SQLDataType::CharacterVarying(_)
            | SQLDataType::CharVarying(_)
            | SQLDataType::CharacterLargeObject(_)
            | SQLDataType::CharLargeObject(_)
            // precision is not supported
            | SQLDataType::Timestamp(Some(_), _)
            // precision is not supported
            | SQLDataType::Time(Some(_), _)
            | SQLDataType::Dec(_)
            | SQLDataType::BigNumeric(_)
            | SQLDataType::BigDecimal(_)
            | SQLDataType::Clob(_)
            | SQLDataType::Bytes(_)
            | SQLDataType::Int16
            | SQLDataType::Int32
            | SQLDataType::Int64
            | SQLDataType::Int128
            | SQLDataType::Int256
            | SQLDataType::UInt8
            | SQLDataType::UInt16
            | SQLDataType::UInt32
            | SQLDataType::UInt64
            | SQLDataType::UInt128
            | SQLDataType::UInt256
            | SQLDataType::Float32
            | SQLDataType::Float64
            | SQLDataType::Date32
            | SQLDataType::Datetime64(_, _)
            | SQLDataType::FixedString(_)
            | SQLDataType::Map(_, _)
            | SQLDataType::FixedString(_)
            | SQLDataType::Tuple(_)
            | SQLDataType::Nested(_)
            | SQLDataType::Union(_)
            | SQLDataType::Nullable(_)
            | SQLDataType::LowCardinality(_)
            | SQLDataType::Trigger
            | SQLDataType::JSONB
            | SQLDataType::Unspecified
            => not_impl_err!(
                "Unsupported SQL type xcvcxv {sql_type:?}"
            ),
        }
    }
}

fn calc_inline_constraints_from_columns(columns: &[ColumnDef]) -> Vec<TableConstraint> {
    let mut constraints = vec![];
    for column in columns {
        for ast::ColumnOptionDef { name, option } in &column.options {
            match option {
                ColumnOption::Unique {
                    is_primary: false,
                    characteristics,
                } => constraints.push(TableConstraint::Unique {
                    name: name.clone(),
                    columns: vec![column.name.clone()],
                    characteristics: *characteristics,
                    index_name: None,
                    index_type_display: ast::KeyOrIndexDisplay::None,
                    index_type: None,
                    index_options: vec![],
                }),
                ColumnOption::Unique {
                    is_primary: true,
                    characteristics,
                } => constraints.push(TableConstraint::PrimaryKey {
                    name: name.clone(),
                    columns: vec![column.name.clone()],
                    characteristics: *characteristics,
                    index_name: None,
                    index_type: None,
                    index_options: vec![],
                }),
                ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    characteristics,
                } => constraints.push(TableConstraint::ForeignKey {
                    name: name.clone(),
                    columns: vec![],
                    foreign_table: foreign_table.clone(),
                    referred_columns: referred_columns.to_vec(),
                    on_delete: *on_delete,
                    on_update: *on_update,
                    characteristics: *characteristics,
                }),
                ColumnOption::Check(expr) => constraints.push(TableConstraint::Check {
                    name: name.clone(),
                    expr: Box::new(expr.clone()),
                }),
                // Other options are not constraint related.
                ColumnOption::Default(_)
                | ColumnOption::Null
                | ColumnOption::NotNull
                | ColumnOption::DialectSpecific(_)
                | ColumnOption::CharacterSet(_)
                | ColumnOption::Generated { .. }
                | ColumnOption::Comment(_)
                | ColumnOption::Options(_)
                | ColumnOption::Materialized(_)
                | ColumnOption::Ephemeral(_)
                | ColumnOption::Alias(_)
                | ColumnOption::OnUpdate(_) => {}
            }
        }
    }
    constraints
}

pub fn make_decimal_type(precision: Option<u64>, scale: Option<u64>) -> Result<DataType> {
    // postgres like behavior
    let (precision, scale) = match (precision, scale) {
        (Some(p), Some(s)) => (p as u8, s as i8),
        (Some(p), None) => (p as u8, 0),
        (None, Some(_)) => return plan_err!("Cannot specify only scale for decimal data type"),
        (None, None) => (DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
    };

    if precision == 0 || precision > DECIMAL256_MAX_PRECISION || scale.unsigned_abs() > precision {
        plan_err!(
            "Decimal(precision = {precision}, scale = {scale}) should satisfy `0 < precision <= 76`, and `scale <= precision`."
        )
    } else if precision > DECIMAL128_MAX_PRECISION && precision <= DECIMAL256_MAX_PRECISION {
        Ok(DataType::Decimal256(precision, scale))
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}
