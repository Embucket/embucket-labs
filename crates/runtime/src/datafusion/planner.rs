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
use datafusion::catalog_common::TableReference;
use datafusion::common::error::_plan_err;
use datafusion::common::{
    not_impl_err, plan_datafusion_err, plan_err, Constraint, Constraints, DFSchema, DFSchemaRef,
    ToDFSchema,
};
use datafusion::common::{DataFusionError, Result, SchemaError};
use datafusion::logical_expr::sqlparser::ast;
use datafusion::logical_expr::sqlparser::ast::{
    ArrayElemTypeDef, ColumnDef, ExactNumberInfo, Ident, ObjectName, TableConstraint,
};
use datafusion::logical_expr::{CreateMemoryTable, DdlStatement, EmptyRelation, LogicalPlan};
use datafusion::prelude::*;
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::planner::{
    object_name_to_table_reference, ContextProvider, IdentNormalizer, PlannerContext, SqlToRel,
};
use datafusion::sql::sqlparser::ast::{
    ColumnDef as SQLColumnDef, ColumnOption, CreateTable as CreateTableStatement,
    DataType as SQLDataType, Statement, TimezoneInfo,
};
use std::sync::Arc;

pub struct ExtendedSqlToRel<'a, S>
where
    S: ContextProvider,
{
    inner: SqlToRel<'a, S>, // The wrapped type
    provider: &'a S,
    ident_normalizer: IdentNormalizer,
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
            ident_normalizer: IdentNormalizer::default(),
        }
    }

    pub fn statement_to_plan(&self, statement: DFStatement) -> Result<LogicalPlan> {
        match statement {
            DFStatement::Statement(s) => self.sql_statement_to_plan(*s),
            _ => self.inner.statement_to_plan(statement),
        }
    }

    /// Custom implementation of `sql_statement_to_plan`
    pub fn sql_statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        // Check for a custom statement type
        match self.handle_custom_statement(statement.clone()) {
            Ok(plan) => return Ok(plan),
            Err(e) => {
                // TODO: Tracing
                eprintln!("Custom statement parsing skipped: {statement}: {e}");
            }
        }

        // For all other statements, delegate to the wrapped SqlToRel
        self.inner.sql_statement_to_plan(statement)
    }

    /// Handle custom statements not supported by the original `SqlToRel`
    fn handle_custom_statement(&self, statement: Statement) -> Result<LogicalPlan> {
        let planner_context: &mut PlannerContext = &mut PlannerContext::new();
        // Example: Custom handling for a specific statement
        match statement.clone() {
            Statement::AlterTable { .. }
            | Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Update { .. } => Ok(LogicalPlan::default()),
            Statement::ShowSchemas { .. } => self.show_variable_to_plan(&["schemas".into()]),
            Statement::ShowVariable { variable } => self.show_variable_to_plan(&variable),
            Statement::CreateTable(CreateTableStatement {
                query,
                name,
                columns,
                constraints,
                table_properties,
                with_options,
                if_not_exists,
                or_replace,
                ..
            }) if table_properties.is_empty() && with_options.is_empty() => {
                // Merge inline constraints and existing constraints
                let mut all_constraints = constraints;
                let inline_constraints = calc_inline_constraints_from_columns(&columns);
                all_constraints.extend(inline_constraints);
                // Build column default values
                let column_defaults = self.build_column_defaults(&columns, planner_context)?;
                // println!("column_defaults: {:?}", column_defaults);
                // println!("statement 11: {:?}", statement);
                let has_columns = !columns.is_empty();
                let schema = self.build_schema(columns.clone())?.to_dfschema_ref()?;
                if has_columns {
                    planner_context.set_table_schema(Some(Arc::clone(&schema)));
                }

                if query.is_some() {
                    self.inner.sql_statement_to_plan(statement)
                } else {
                    let plan = EmptyRelation {
                        produce_one_row: false,
                        schema,
                    };
                    let plan = LogicalPlan::EmptyRelation(plan);
                    let constraints = Self::new_constraint_from_table_constraints(
                        &all_constraints,
                        plan.schema(),
                    )?;
                    Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                        CreateMemoryTable {
                            name: object_name_to_table_reference(name, true)?,
                            constraints,
                            input: Arc::new(plan),
                            if_not_exists,
                            or_replace,
                            column_defaults,
                            temporary: false,
                        },
                    )))
                }
            }
            _ => plan_err!("Unsupported statement: {:?}", statement),
        }
    }

    /// Returns a vector of (`column_name`, `default_expr`) pairs
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
                column_defaults.push((
                    self.ident_normalizer.normalize(column.name.clone()),
                    default_expr,
                ));
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
            let mut field = Field::new(
                self.ident_normalizer.normalize(column.name),
                data_type,
                !not_nullable,
            );
            // Add metadata for custom data types
            self.add_custom_metadata(&mut field, &column.data_type);
            fields.push(field);
        }

        Ok(Schema::new(fields))
    }

    pub fn convert_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::Array(
                ArrayElemTypeDef::AngleBracket(inner_sql_type)
                | ArrayElemTypeDef::SquareBracket(inner_sql_type, _),
            ) => {
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

    #[allow(clippy::too_many_lines, clippy::match_same_arms)]
    fn convert_simple_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::Boolean | SQLDataType::Bool => Ok(DataType::Boolean),
            SQLDataType::TinyInt(_) => Ok(DataType::Int8),
            SQLDataType::SmallInt(_) | SQLDataType::Int2(_)| SQLDataType::Int16 => Ok(DataType::Int16),
            SQLDataType::Int(_)
            | SQLDataType::Integer(_)
            | SQLDataType::Int4(_)
            | SQLDataType::Int32 => Ok(DataType::Int32) ,
            SQLDataType::BigInt(_) | SQLDataType::Int8(_) | SQLDataType::Int64 => Ok(DataType::Int64),
            SQLDataType::UnsignedTinyInt(_) | SQLDataType::UInt8 => Ok(DataType::UInt8),
            SQLDataType::UnsignedSmallInt(_)
            | SQLDataType::UnsignedInt2(_)
            | SQLDataType::UInt16 => Ok(DataType::UInt16),
            SQLDataType::UnsignedInt(_)
            | SQLDataType::UnsignedInteger(_)
            | SQLDataType::UnsignedInt4(_)
            | SQLDataType::UInt32 => Ok(DataType::UInt32),
            SQLDataType::Varchar(length) => match (length, true) {
                (Some(_), false) => plan_err!(
                    "does not support Varchar with length, please set `support_varchar_with_length` to be true"
                ),
                _ => Ok(DataType::Utf8),
            },
            SQLDataType::Blob(_) => Ok(DataType::Binary),
            SQLDataType::UnsignedBigInt(_)
            | SQLDataType::UnsignedInt8(_)
            | SQLDataType::UInt64 => Ok(DataType::UInt64),
            SQLDataType::Real
            | SQLDataType::Float4
            | SQLDataType::Float(_)
            | SQLDataType::Float32=> Ok(DataType::Float32),
            SQLDataType::Double
            | SQLDataType::DoublePrecision
            | SQLDataType::Float8
            | SQLDataType::Float64 => Ok(DataType::Float64),
            SQLDataType::Char(_) | SQLDataType::Text | SQLDataType::String(_) => Ok(DataType::Utf8),
            SQLDataType::Timestamp(precision, tz_info) => {
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
                let precision = match precision {
                    Some(0) => TimeUnit::Second,
                    Some(3) => TimeUnit::Millisecond,
                    Some(6) => TimeUnit::Microsecond,
                    None | Some(9) => TimeUnit::Nanosecond,
                    _ => unreachable!(),
                };
                Ok(DataType::Timestamp(precision, tz.map(Into::into)))
            }
            SQLDataType::Date => Ok(DataType::Date32),
            SQLDataType::Time(None, tz_info) => {
                if matches!(tz_info, TimezoneInfo::None)
                    || matches!(tz_info, TimezoneInfo::WithoutTimeZone)
                {
                    Ok(DataType::Time64(TimeUnit::Nanosecond))
                } else {
                    // We dont support TIMETZ and TIME WITH TIME ZONE for now
                    not_impl_err!("Unsupported SQL type {sql_type:?}")
                }
            }
            SQLDataType::Numeric(exact_number_info) | SQLDataType::Decimal(exact_number_info) => {
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
                        let field_name = field.field_name.as_ref().map_or_else(
                            || Ident::new(format!("c{idx}")),
                            Clone::clone,
                        );
                        Ok(Arc::new(Field::new(
                            self.ident_normalizer.normalize(field_name),
                            data_type,
                            true,
                        )))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataType::Struct(Fields::from(fields)))
            }
            // https://github.com/apache/datafusion/issues/12644
            SQLDataType::JSON => Ok(DataType::Utf8),
            SQLDataType::Custom(a, b) => match a.to_string().to_uppercase().as_str() {
                "VARIANT" => Ok(DataType::Utf8),
                "TIMESTAMP_NTZ" => {
                    let parsed_b: Option<u64> = b.iter().next().and_then(|s| s.parse().ok());
                    match parsed_b {
                        Some(0) => Ok(DataType::Timestamp(TimeUnit::Second, None)),
                        Some(3) => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
                        Some(6) => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
                        Some(9) => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                        _ => not_impl_err!("Unsupported SQL TIMESTAMP_NZT precision {parsed_b:?}"),
                    }
                }
                "NUMBER" => {
                    let (precision, scale) = match b.len() {
                        0 => (None, None),
                        1 => {
                            let precision = b[0].parse().map_err(|_| {
                                DataFusionError::Plan(format!("Invalid precision: {}", b[0]))
                            })?;
                            (Some(precision), None)
                        }
                        2 => {
                            let precision = b[0].parse().map_err(|_| {
                                DataFusionError::Plan(format!("Invalid precision: {}", b[0]))
                            })?;
                            let scale = b[1].parse().map_err(|_| {
                                DataFusionError::Plan(format!("Invalid scale: {}", b[1]))
                            })?;
                            (Some(precision), Some(scale))
                        }
                        _ => {
                            return Err(DataFusionError::Plan(format!(
                                "Invalid NUMBER type format: {b:?}"
                            )));
                        }
                    };
                    make_decimal_type(precision, scale)
                }
                _ => Ok(DataType::Utf8),
            },
            // Explicitly list all other types so that if sqlparser
            // adds/changes the `SQLDataType` the compiler will tell us on upgrade
            // and avoid bugs like https://github.com/apache/datafusion/issues/3059
            SQLDataType::Nvarchar(_)
            | SQLDataType::Uuid
            | SQLDataType::Binary(_)
            | SQLDataType::Varbinary(_)
            | SQLDataType::Datetime(_)
            | SQLDataType::Regclass
            | SQLDataType::Array(_)
            | SQLDataType::Enum(_, _)
            | SQLDataType::Set(_)
            | SQLDataType::MediumInt(_)
            | SQLDataType::UnsignedMediumInt(_)
            | SQLDataType::Character(_)
            | SQLDataType::CharacterVarying(_)
            | SQLDataType::CharVarying(_)
            | SQLDataType::CharacterLargeObject(_)
            | SQLDataType::CharLargeObject(_)
            | SQLDataType::Time(Some(_), _)
            | SQLDataType::Dec(_)
            | SQLDataType::BigNumeric(_)
            | SQLDataType::BigDecimal(_)
            | SQLDataType::Clob(_)
            | SQLDataType::Bytes(_)
            | SQLDataType::Int128
            | SQLDataType::Int256
            | SQLDataType::UInt128
            | SQLDataType::UInt256
            | SQLDataType::Date32
            | SQLDataType::Datetime64(_, _)
            | SQLDataType::FixedString(_)
            | SQLDataType::Map(_, _)
            | SQLDataType::Tuple(_)
            | SQLDataType::Nested(_)
            | SQLDataType::Union(_)
            | SQLDataType::Nullable(_)
            | SQLDataType::LowCardinality(_)
            | SQLDataType::Trigger
            | SQLDataType::JSONB
            | SQLDataType::TinyBlob
            | SQLDataType::MediumBlob
            | SQLDataType::LongBlob
            | SQLDataType::TinyText
            | SQLDataType::MediumText
            | SQLDataType::LongText
            | SQLDataType::Bit(_)
            | SQLDataType::BitVarying(_)
            | SQLDataType::Unspecified => not_impl_err!("Unsupported SQL type {sql_type:?}"),
        }
    }

    pub fn add_custom_metadata(&self, field: &mut Field, sql_type: &SQLDataType) {
        match sql_type {
            SQLDataType::JSON => {
                *field = field.clone().with_metadata(
                    std::iter::once(&("type".to_string(), "JSON".to_string()))
                        .cloned()
                        .collect(),
                );
            }
            SQLDataType::Custom(a, _b) => {
                if a.to_string().to_uppercase() == "VARIANT" {
                    *field = field.clone().with_metadata(
                        std::iter::once(&("type".to_string(), "VARIANT".to_string()))
                            .cloned()
                            .collect(),
                    );
                }
            }
            _ => {}
        }
    }

    fn new_constraint_from_table_constraints(
        constraints: &[TableConstraint],
        df_schema: &DFSchemaRef,
    ) -> Result<Constraints> {
        let constraints = constraints
            .iter()
            .map(|c: &TableConstraint| match c {
                TableConstraint::Unique { name, columns, .. } => {
                    let field_names = df_schema.field_names();
                    // Get unique constraint indices in the schema:
                    let indices = columns
                        .iter()
                        .map(|u| {
                            let idx = field_names
                                .iter()
                                .position(|item| *item.to_lowercase() == u.value.to_lowercase())
                                .ok_or_else(|| {
                                    let name = name.as_ref().map_or(String::new(), |name| {
                                        format!("with name '{name}' ")
                                    });
                                    DataFusionError::Execution(format!(
                                        "Column for unique constraint {}not found in schema: {}",
                                        name, u.value
                                    ))
                                })?;
                            Ok(idx)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Constraint::Unique(indices))
                }
                TableConstraint::PrimaryKey { columns, .. } => {
                    let field_names = df_schema.field_names();
                    // Get primary key indices in the schema:
                    let indices = columns
                        .iter()
                        .map(|pk| {
                            let idx = field_names
                                .iter()
                                .position(|item| *item.to_lowercase() == pk.value.to_lowercase())
                                .ok_or_else(|| {
                                    DataFusionError::Execution(format!(
                                        "Column for primary key not found in schema: {}",
                                        pk.value
                                    ))
                                })?;
                            Ok(idx)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Constraint::PrimaryKey(indices))
                }
                TableConstraint::ForeignKey { .. } => {
                    _plan_err!("Foreign key constraints are not currently supported")
                }
                TableConstraint::Check { .. } => {
                    _plan_err!("Check constraints are not currently supported")
                }
                TableConstraint::FulltextOrSpatial { .. } | TableConstraint::Index { .. } => {
                    _plan_err!("Indexes are not currently supported")
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Constraints::new_unverified(constraints))
    }

    fn show_variable_to_plan(&self, variable: &[Ident]) -> Result<LogicalPlan> {
        //println!("SHOW variable: {:?}", variable);
        if !self.has_table("information_schema", "df_settings") {
            return plan_err!(
                "SHOW [VARIABLE] is not supported unless information_schema is enabled"
            );
        }

        let verbose = variable
            .last()
            .is_some_and(|s| ident_to_string(s) == "verbose");
        let mut variable_vec = variable.to_vec();
        let mut columns: String = "name, value".to_owned();

        // TODO: Fix how columns are selected. Vec instead of string
        #[allow(unused_assignments)]
        if verbose {
            columns = format!("{columns}, description");
            variable_vec = variable_vec.split_at(variable_vec.len() - 1).0.to_vec();
        }

        let query = if variable_vec.iter().any(|ident| ident.value == "objects") {
            columns = [
                "table_catalog as 'database_name'",
                "table_schema as 'schema_name'",
                "table_name as 'name'",
                "case when table_type='BASE TABLE' then 'TABLE' else table_type end as 'kind'",
                "null as 'comment'",
            ]
            .join(", ");
            format!("SELECT {columns} FROM information_schema.tables")
        } else {
            let variable = object_name_to_string(&ObjectName(variable_vec));
            // let base_query = format!("SELECT {columns} FROM information_schema.df_settings");
            let base_query = "select schema_name as 'name' from information_schema.schemata";
            let query_res = if variable == "all" {
                // Add an ORDER BY so the output comes out in a consistent order
                format!("{base_query} ORDER BY name")
            } else if variable == "timezone" || variable == "time.zone" {
                // we could introduce alias in OptionDefinition if this string matching thing grows
                format!("{base_query} WHERE name = 'datafusion.execution.time_zone'")
            } else {
                // These values are what are used to make the information_schema table, so we just
                // check here, before actually planning or executing the query, if it would produce no
                // results, and error preemptively if it would (for a better UX)
                let is_valid_variable = self
                    .provider
                    .options()
                    .entries()
                    .iter()
                    .any(|opt| opt.key == variable);

                if is_valid_variable {
                    format!("{base_query} WHERE name = '{variable}'")
                } else {
                    // skip where clause to return empty result
                    base_query.to_string()
                }
            };
            query_res
        };

        let mut statements = DFParser::parse_sql(query.as_str())?;
        statements.pop_front().map_or_else(
            || plan_err!("Failed to parse SQL statement"),
            |statement| {
                if let DFStatement::Statement(s) = statement {
                    self.sql_statement_to_plan(*s)
                } else {
                    plan_err!("Failed to parse SQL statement")
                }
            },
        )
    }

    fn has_table(&self, schema: &str, table: &str) -> bool {
        let tables_reference = TableReference::Partial {
            schema: schema.into(),
            table: table.into(),
        };
        self.provider.get_table_source(tables_reference).is_ok()
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
                    nulls_distinct: ast::NullsDistinctOption::None,
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
                    referred_columns: referred_columns.clone(),
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
                | ColumnOption::OnUpdate(_)
                | ColumnOption::Identity(_)
                | ColumnOption::OnConflict(_)
                | ColumnOption::Policy(_)
                | ColumnOption::Tags(_) => {}
            }
        }
    }
    constraints
}

#[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
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

fn ident_to_string(ident: &Ident) -> String {
    normalize_ident(ident.to_owned())
}

fn object_name_to_string(object_name: &ObjectName) -> String {
    object_name
        .0
        .iter()
        .map(ident_to_string)
        .collect::<Vec<String>>()
        .join(".")
}

// Normalize an owned identifier to a lowercase string unless the identifier is quoted.
#[must_use]
pub fn normalize_ident(id: Ident) -> String {
    match id.quote_style {
        Some(_) => id.value,
        None => id.value.to_ascii_lowercase(),
    }
}
