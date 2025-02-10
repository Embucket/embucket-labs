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

use arrow::datatypes::{Field, Schema};
use datafusion::common::Result;
use datafusion::common::{plan_err, ToDFSchema};
use datafusion::logical_expr::sqlparser::ast::{Ident, ObjectName};
use datafusion::logical_expr::{CreateMemoryTable, DdlStatement, EmptyRelation, LogicalPlan};
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::planner::{
    object_name_to_table_reference, ContextProvider, IdentNormalizer, ParserOptions,
    PlannerContext, SqlToRel,
};
use datafusion::sql::sqlparser::ast::{
    ColumnDef as SQLColumnDef, ColumnOption, CreateTable as CreateTableStatement,
    DataType as SQLDataType, Statement,
};
use datafusion::sql::statement::{calc_inline_constraints_from_columns, object_name_to_string};
use datafusion_common::{DFSchema, DFSchemaRef, SchemaReference, TableReference};
use datafusion_expr::DropCatalogSchema;
use sqlparser::ast::ObjectType;
use std::sync::Arc;

pub struct ExtendedSqlToRel<'a, S>
where
    S: ContextProvider,
{
    inner: SqlToRel<'a, S>, // The wrapped type
    provider: &'a S,
    options: ParserOptions,
    ident_normalizer: IdentNormalizer,
}

impl<'a, S> ExtendedSqlToRel<'a, S>
where
    S: ContextProvider,
{
    /// Create a new instance of `ExtendedSqlToRel`
    pub fn new(provider: &'a S, options: ParserOptions) -> Self {
        let ident_normalize = options.enable_ident_normalization;

        Self {
            inner: SqlToRel::new(provider),
            provider,
            options,
            ident_normalizer: IdentNormalizer::new(ident_normalize),
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
    #[allow(clippy::too_many_lines)]
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
            Statement::ShowObjects {
                terse: _,
                show_options,
            } => {
                let Some(show_in) = show_options.show_in else {
                    return plan_err!("Unsupported show statement: missing show_in");
                };
                let Some(parent_name) = show_in.parent_name else {
                    return plan_err!("Unsupported show statement: missing parent_name");
                };
                self.show_objects_to_plan(&parent_name)
            }
            Statement::Drop {
                object_type,
                if_exists,
                mut names,
                cascade,
                ..
            } => match object_type {
                ObjectType::Database => {
                    #[allow(clippy::unwrap_used)]
                    let name = object_name_to_table_reference(
                        names.pop().unwrap(),
                        self.options.enable_ident_normalization,
                    )?;
                    let schema_name = match name {
                        TableReference::Bare { table } => {
                            Ok(SchemaReference::Bare { schema: table })
                        }
                        TableReference::Partial { schema, table } => Ok(SchemaReference::Full {
                            schema: table,
                            catalog: schema,
                        }),
                        TableReference::Full { .. } => {
                            plan_err!("Invalid schema specifier (has 3 parts)")
                        }
                    }?;

                    Ok(LogicalPlan::Ddl(DdlStatement::DropCatalogSchema(
                        DropCatalogSchema {
                            name: schema_name,
                            if_exists,
                            cascade,
                            schema: DFSchemaRef::new(DFSchema::empty()),
                        },
                    )))
                }
                _ => plan_err!("Unsupported drop: {:?}", object_type),
            },
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
                let column_defaults = self
                    .inner
                    .build_column_defaults(&columns, planner_context)?;
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
                    let constraints = SqlToRel::<S>::new_constraint_from_table_constraints(
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

    pub fn build_schema(&self, columns: Vec<SQLColumnDef>) -> Result<Schema> {
        let mut fields = Vec::with_capacity(columns.len());

        for column in columns {
            let data_type = self.inner.convert_data_type(&column.data_type)?;
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

    fn show_objects_to_plan(&self, parent: &ObjectName) -> Result<LogicalPlan> {
        if !self.inner.has_table("information_schema", "df_settings") {
            return plan_err!("SHOW OBJECTS is not supported unless information_schema is enabled");
        }
        // Only support listing objects in schema for now
        match parent.0.len() {
            2 => {
                // let (catalog, schema) = (parent.0[0].value.clone(), parent.0[1].value.clone());

                // Create query to list objects in schema
                let columns = [
                    "table_catalog as 'database_name'",
                    "table_schema as 'schema_name'",
                    "table_name as 'name'",
                    "case when table_type='BASE TABLE' then 'TABLE' else table_type end as 'kind'",
                    "case when table_type='BASE TABLE' then 'Y' else 'N' end as 'is_iceberg'",
                    "null as 'comment'",
                ]
                .join(", ");
                // TODO: views?
                // TODO: Return programmatically constructed plan
                let query = format!("SELECT {columns} FROM information_schema.tables");
                self.parse_sql(query.as_str())
            }
            _ => plan_err!("Unsupported show objects: {:?}", parent),
        }
    }

    fn show_variable_to_plan(&self, variable: &[Ident]) -> Result<LogicalPlan> {
        if !self.inner.has_table("information_schema", "df_settings") {
            return plan_err!(
                "SHOW [VARIABLE] is not supported unless information_schema is enabled"
            );
        }

        let variable = object_name_to_string(&ObjectName(variable.to_vec()));
        // let base_query = format!("SELECT {columns} FROM information_schema.df_settings");
        let base_query = "select schema_name as 'name' from information_schema.schemata";
        let query = if variable == "all" {
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
        self.parse_sql(query.as_str())
    }

    fn parse_sql(&self, sql: &str) -> Result<LogicalPlan> {
        let mut statements = DFParser::parse_sql(sql)?;
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
}
