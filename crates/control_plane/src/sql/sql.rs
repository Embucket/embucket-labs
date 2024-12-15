use crate::sql::context::CustomContextProvider;
use crate::sql::functions::parse_json::ParseJsonFunc;
use crate::sql::planner::ExtendedSqlToRel;
use arrow::array::RecordBatch;
use datafusion::catalog::CatalogProvider;
use datafusion::common::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{LogicalPlan, ScalarUDF};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{ObjectName, Statement, CreateTable as CreateTableStatement, SetExpr, Query, Value, GroupByExpr};
use datafusion::datasource::default_table_source::provider_as_source;
use iceberg_rust::catalog::create::CreateTable as CreateTableCatalog;
use datafusion_functions_json::register_all;
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::spec::identifier::Identifier;
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::StructType;
use std::collections::HashMap;
use datafusion::sql::sqlparser::ast::{ TableFactor, TableWithJoins, Select, Expr, BinaryOperator, SelectItem, Ident};


pub struct SqlExecutor {
    ctx: SessionContext,
}

impl SqlExecutor {
    pub fn new(mut ctx: SessionContext) -> Self {
        ctx.register_udf(ScalarUDF::from(ParseJsonFunc::new()));
        register_all(&mut ctx).expect("Cannot register UDF JSON funcs");
        Self { ctx }
    }

    pub async fn query(&self, query: &String, warehouse_name: &String) -> Result<Vec<RecordBatch>> {
        let state = self.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();
        let mut statement = state.sql_to_statement(query, dialect)?;

        if let DFStatement::Statement(ref mut s) = statement {
            println!("old statement: {}", s.clone().to_string());
            if let Statement::CreateTable (CreateTableStatement { ref mut query, .. }) = &mut **s {
                if let Some(ref mut query) = query {
                    self.modify_selects_with_qualify(query);
                }
                println!("new statement: {}", s.clone().to_string());
                return self.create_table_query(*s.clone(), warehouse_name).await;
            }
        }

        self.ctx.sql(query).await?.collect().await
    }

    pub async fn create_table_query(
        &self,
        statement: Statement,
        warehouse_name: &String,
    ) -> Result<Vec<RecordBatch>> {
        if let Statement::CreateTable(CreateTableStatement {
                                          or_replace,
                                          temporary,
                                          external,
            global,
            if_not_exists,
            transient,
            volatile, name,
            columns,
            constraints,
            hive_distribution,
            hive_formats,
            table_properties,
            with_options,
            file_format,
            location,
            query,
            without_rowid,
            like,
            clone,
            engine,
            comment,
            auto_increment_offset,
            default_charset,
            collation,
            on_commit,
            on_cluster,
            primary_key,
            order_by,
            partition_by,
            cluster_by,
            clustered_by,
            options,
            strict,
            copy_grants,
            enable_schema_evolution,
            change_tracking,
            data_retention_time_in_days,
            max_data_extension_time_in_days,
            default_ddl_collation,
            with_aggregation_policy,
            with_row_access_policy,
            with_tags,
           }) = statement {
            // Split table that needs to be created into parts
            let new_table_full_name = name.to_string();
            let new_table_wh_id = name.0[0].clone();
            let new_table_db = name.0[1].clone();
            let new_table_name = name.0[2].clone();

            // Replace the name of table that needs creation (for ex. "warehouse"."database"."table" -> "table")
            // And run the query - this will create an InMemory table
            let modified_statement = Statement::CreateTable(CreateTableStatement {
                or_replace,
                temporary,
                external,
                global,
                if_not_exists,
                transient,
                volatile,
                columns,
                constraints,
                hive_distribution,
                hive_formats,
                table_properties,
                with_options,
                file_format,
                query,
                without_rowid,
                like,
                clone,
                engine,
                comment,
                auto_increment_offset,
                default_charset,
                collation,
                on_commit,
                on_cluster,
                primary_key,
                order_by,
                partition_by,
                cluster_by,
                clustered_by,
                options,
                strict,
                copy_grants,
                enable_schema_evolution,
                change_tracking,
                data_retention_time_in_days,
                max_data_extension_time_in_days,
                default_ddl_collation,
                with_aggregation_policy,
                with_row_access_policy,
                with_tags,
                location: location.clone(),
                name: ObjectName {
                    0: vec![new_table_name.clone()],
                },
            });
            let updated_query = modified_statement.to_string();
            self.execute_with_custom_plan(&updated_query).await?;

            // Get schema of new table
            let plan = self.get_custom_logical_plan(&updated_query).await?;
            let schema = Schema::builder()
                .with_schema_id(0)
                .with_identifier_field_ids(vec![])
                .with_fields(StructType::try_from(plan.schema().as_arrow()).unwrap())
                .build()
                .unwrap();

            // Check if it already exists, if it is - drop it
            // For now we behave as CREATE OR REPLACE
            // TODO support CREATE without REPLACE
            let catalog = self.ctx.catalog(warehouse_name).unwrap();
            let iceberg_catalog = catalog.as_any().downcast_ref::<IcebergCatalog>().unwrap();
            let rest_catalog = iceberg_catalog.catalog();
            let new_table_ident = Identifier::new(&[new_table_db.value], &*new_table_name.value);
            match rest_catalog.tabular_exists(&new_table_ident).await {
                Ok(true) => {
                    rest_catalog.drop_table(&new_table_ident).await.unwrap();
                }
                Ok(false) => {}
                Err(_) => {}
            };

            // Create new table
            rest_catalog
                .create_table(
                    new_table_ident.clone(),
                    CreateTableCatalog {
                        name: new_table_name.value.clone(),
                        location,
                        schema,
                        partition_spec: None,
                        write_order: None,
                        stage_create: None,
                        properties: None,
                    },
                )
                .await
                .unwrap();

            // Copy data from InMemory table to created table
            let insert_query =
                format!("INSERT INTO {new_table_full_name} SELECT * FROM {new_table_name}");
            let result = self.ctx.sql(&insert_query).await?.collect().await?;

            // Drop InMemory table
            let drop_query = format!("DROP TABLE {new_table_name}");
            self.ctx.sql(&drop_query).await?.collect().await?;

            Ok(result)
        } else {
            Err(datafusion::error::DataFusionError::NotImplemented(
                "Only CREATE TABLE statements are supported".to_string(),
            ))
        }
    }

    pub async fn get_custom_logical_plan(&self, query: &String) -> Result<LogicalPlan> {
        let state = self.ctx.state();
        let dialect = state.config().options().sql_parser.dialect.as_str();
        let statement = state.sql_to_statement(query, dialect)?;

        if let DFStatement::Statement(s) = statement {
            let mut ctx_provider = CustomContextProvider {
                state: &state,
                tables: HashMap::new(),
            };
            for catalog in self.ctx.state().catalog_list().catalog_names() {
                let provider = self.ctx.state().catalog_list().catalog(&catalog).unwrap();
                for schema in provider.schema_names() {
                    for table in provider.schema(&schema).unwrap().table_names() {
                        let table_source = provider
                            .schema(&schema)
                            .unwrap()
                            .table(&table)
                            .await
                            .unwrap()
                            .unwrap();
                        ctx_provider.tables.insert(
                            format!("{catalog}.{schema}.{table}"),
                            provider_as_source(table_source),
                        );
                    }
                }
            }
            let planner = ExtendedSqlToRel::new(&ctx_provider);
            planner.sql_statement_to_plan(*s)
        } else {
            Err(datafusion::error::DataFusionError::NotImplemented(
                "Only SQL statements are supported".to_string(),
            ))
        }
    }

    pub async fn execute_with_custom_plan(&self, query: &String) -> Result<Vec<RecordBatch>> {
        let plan = self.get_custom_logical_plan(query).await?;
        self.ctx.execute_logical_plan(plan).await?.collect().await
    }

    fn modify_selects_with_qualify(&self, query: &mut Query) {
        match &mut *query.body {
            SetExpr::Select(select) => {
                if let Some(qualify_expr) = &select.qualify {
                    if let Expr::BinaryOp { left, op, right } = qualify_expr {
                        if matches!(op, BinaryOperator::Eq) {
                            let mut inner_select = select.clone();
                            inner_select.projection.push(
                                SelectItem::ExprWithAlias {
                                    expr: *(left.clone()),
                                    alias: Ident { value: "qualify_alias".to_string(), quote_style: None }
                                }
                            );
                            inner_select.qualify = None;
                            let subquery = Query {
                                with: None,
                                body: Box::new(SetExpr::Select(inner_select)),
                                order_by: None,
                                limit: None,
                                limit_by: vec![],
                                offset: None,
                                fetch: None,
                                locks: vec![],
                                for_clause: None,
                                settings: None,
                                format_clause: None,
                            };
                            let outer_select = Select {
                                distinct: None,
                                top: None,
                                projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident { value: "*".to_string(),
                                    quote_style: None }))],
                                into: None,
                                from: vec![TableWithJoins {
                                    relation: TableFactor::Derived {
                                        lateral: false,
                                        subquery: Box::new(subquery),
                                        alias: None,
                                    },
                                    joins: vec![],
                                }],
                                lateral_views: vec![],
                                prewhere: None,
                                selection: Some(Expr::BinaryOp {
                                    left: Box::new(Expr::Identifier(Ident { value: "qualify_alias".to_string(), quote_style: None })),
                                    op: BinaryOperator::Eq,
                                    right: Box::new(*right.clone())
                                }),
                                group_by: GroupByExpr::Expressions(vec![], vec![]),
                                cluster_by: vec![],
                                distribute_by: vec![],
                                sort_by: vec![],
                                having: None,
                                named_window: vec![],
                                qualify: None,
                                window_before_qualify: false,
                                value_table_mode: None,
                                connect_by: None,
                            };

                            *query.body = SetExpr::Select(Box::new(outer_select));
                        } else {
                            println!("QUALIFY expression is a binary operation, but not an equality.");
                        }
                    } else {
                        println!("QUALIFY expression is not a binary operation.");
                    }
                }
            }
            SetExpr::Query(inner_query) => {
                self.modify_selects_with_qualify(inner_query);
            }
            // Handle any other query patterns if required
            _ => {}
        }

        // Recursively check the CTEs (Common Table Expressions) and modify them if applicable
        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                self.modify_selects_with_qualify(&mut cte.query);
            }
        }
    }
}
