use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{Result, ToDFSchema};
use datafusion::logical_expr::{CreateMemoryTable, DdlStatement, EmptyRelation, LogicalPlan};
use datafusion::sql::planner::{
    ContextProvider, IdentNormalizer, ParserOptions, PlannerContext, SqlToRel,
    object_name_to_table_reference,
};
use datafusion::sql::sqlparser::ast::{
    ColumnDef as SQLColumnDef, ColumnOption, CreateTable as CreateTableStatement,
    DataType as SQLDataType, Statement,
};
use datafusion::sql::statement::calc_inline_constraints_from_columns;
use std::sync::Arc;

pub struct ExtendedSqlToRel<'a, S>
where
    S: ContextProvider,
{
    inner: SqlToRel<'a, S>, // The wrapped type
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
            ident_normalizer: IdentNormalizer::new(ident_normalize),
        }
    }

    /// Handle custom statements not supported by the original `SqlToRel`
    #[allow(clippy::too_many_lines)]
    pub fn sql_statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        let planner_context: &mut PlannerContext = &mut PlannerContext::new();
        match statement.clone() {
            Statement::CreateTable(CreateTableStatement {
                query,
                name,
                columns,
                constraints,
                if_not_exists,
                or_replace,
                ..
            }) => {
                // Merge inline constraints and existing constraints
                let mut all_constraints = constraints;
                let inline_constraints = calc_inline_constraints_from_columns(&columns);
                all_constraints.extend(inline_constraints);
                // Build column default values
                let column_defaults = self
                    .inner
                    .build_column_defaults(&columns, planner_context)?;
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
                    let constraints = self
                        .inner
                        .new_constraint_from_table_constraints(&all_constraints, plan.schema())?;
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
            _ => self.inner.sql_statement_to_plan(statement),
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
}

impl<'a, S> AsRef<SqlToRel<'a, S>> for ExtendedSqlToRel<'a, S>
where
    S: ContextProvider,
{
    fn as_ref(&self) -> &SqlToRel<'a, S> {
        &self.inner
    }
}
