use super::columns::InformationSchemaColumnsBuilder;
use super::df_settings::{ConfigVisitor, InformationSchemaDfSettingsBuilder};
use super::information_schema::INFORMATION_SCHEMA;
use super::parameters::{
    InformationSchemaParametersBuilder, get_udaf_args_and_return_types,
    get_udf_args_and_return_types, get_udwf_args_and_return_types,
};
use super::routines::InformationSchemaRoutinesBuilder;
use super::schemata::InformationSchemataBuilder;
use super::tables::InformationSchemaTablesBuilder;
use super::views::InformationSchemaViewBuilder;
use crate::catalog::CachingCatalog;
use crate::df_error;
use crate::information_schema::databases::InformationSchemaDatabasesBuilder;
use crate::information_schema::navigation_tree::InformationSchemaNavigationTreeBuilder;
use dashmap::DashMap;
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::CatalogProviderList;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use datafusion_common::DataFusionError;
use datafusion_common::config::ConfigOptions;
use datafusion_doc::Documentation;
use datafusion_expr::{AggregateUDF, ScalarUDF, TableType, WindowUDF};
use embucket_functions::session_params::SessionParams;
use snafu::ResultExt;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct InformationSchemaConfig {
    pub(crate) catalog_list: Arc<dyn CatalogProviderList>,
    pub(crate) catalog_name: Arc<str>,
    pub(crate) views_schemas: DashMap<String, Arc<Schema>>,
}

impl InformationSchemaConfig {
    /// Construct the `information_schema.tables` virtual table
    pub(crate) async fn make_tables(
        &self,
        builder: &mut InformationSchemaTablesBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        let Some(catalog) = self.catalog_list.catalog(&self.catalog_name) else {
            return Ok(());
        };

        for schema_name in catalog
            .schema_names()
            .into_iter()
            .filter(|s| s != INFORMATION_SCHEMA)
        {
            let Some(schema) = catalog.schema(&schema_name) else {
                continue;
            };
            for table_name in schema.table_names() {
                if let Some(table) = schema.table(&table_name).await? {
                    builder.add_table(
                        &self.catalog_name,
                        &schema_name,
                        &table_name,
                        table.table_type(),
                    );
                }
            }
        }

        for table in &self.views_schemas {
            builder.add_table(
                &self.catalog_name,
                INFORMATION_SCHEMA,
                table.key(),
                TableType::View,
            );
        }

        Ok(())
    }

    pub(crate) async fn make_navigation_tree(
        &self,
        builder: &mut InformationSchemaNavigationTreeBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        for catalog_name in self.catalog_list.catalog_names() {
            let catalog = self.catalog_list.catalog(&catalog_name).ok_or_else(|| {
                df_error::CatalogNotFoundSnafu {
                    name: catalog_name.clone(),
                }
                .build()
            })?;
            builder.add_navigation_tree(&catalog_name, None, None, None);

            for schema_name in catalog
                .schema_names()
                .into_iter()
                .filter(|s| s != INFORMATION_SCHEMA)
            {
                builder.add_navigation_tree(&catalog_name, Some(schema_name.clone()), None, None);
                let Some(schema) = catalog.schema(&schema_name) else {
                    continue;
                };
                for table_name in schema.table_names() {
                    if let Some(table) = schema.table(&table_name).await? {
                        builder.add_navigation_tree(
                            &catalog_name,
                            Some(schema_name.clone()),
                            Some(table_name),
                            Some(table.table_type()),
                        );
                    }
                }
            }

            // Add a final list for the information schema tables themselves
            for table in &self.views_schemas {
                builder.add_navigation_tree(
                    &catalog_name,
                    Some(INFORMATION_SCHEMA.to_string()),
                    Some(table.key().to_string()),
                    Some(TableType::View),
                );
            }
        }

        Ok(())
    }

    pub(crate) fn make_schemata(&self, builder: &mut InformationSchemataBuilder) {
        if let Some(catalog) = self.catalog_list.catalog(&self.catalog_name) {
            for schema_name in catalog.schema_names() {
                if schema_name == INFORMATION_SCHEMA {
                    continue;
                }

                if let Some(schema) = catalog.schema(&schema_name) {
                    let schema_owner = schema.owner_name();
                    builder.add_schemata(&self.catalog_name, &schema_name, schema_owner);
                }
            }
        }
        builder.add_schemata(&self.catalog_name, INFORMATION_SCHEMA, None);
    }

    pub(crate) async fn make_views(
        &self,
        builder: &mut InformationSchemaViewBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        if let Some(catalog) = self.catalog_list.catalog(&self.catalog_name) {
            for schema_name in catalog.schema_names() {
                if schema_name == INFORMATION_SCHEMA {
                    continue;
                }
                if let Some(schema) = catalog.schema(&schema_name) {
                    for table_name in schema.table_names() {
                        if let Some(table) = schema.table(&table_name).await?
                            && table.table_type() == TableType::View
                        {
                            builder.add_view(
                                &self.catalog_name,
                                &schema_name,
                                &table_name,
                                table.table_type(),
                                table.get_table_definition(),
                            );
                        }
                    }
                }
            }
        }

        for table in &self.views_schemas {
            builder.add_view(
                &self.catalog_name,
                INFORMATION_SCHEMA,
                table.key(),
                TableType::View,
                None,
            );
        }

        Ok(())
    }

    /// Construct the `information_schema.columns` virtual table
    pub(crate) async fn make_columns(
        &self,
        builder: &mut InformationSchemaColumnsBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        if let Some(catalog) = self.catalog_list.catalog(&self.catalog_name) {
            for schema_name in catalog.schema_names() {
                if schema_name == INFORMATION_SCHEMA {
                    continue;
                }
                if let Some(schema) = catalog.schema(&schema_name) {
                    for table_name in schema.table_names() {
                        if let Some(table) = schema.table(&table_name).await? {
                            for (field_position, field) in
                                table.schema().fields().iter().enumerate()
                            {
                                builder.add_column(
                                    &self.catalog_name,
                                    &schema_name,
                                    &table_name,
                                    table.table_type(),
                                    field_position,
                                    field,
                                );
                            }
                        }
                    }
                }
            }
        }

        // Add a final list for the information schema tables themselves
        for table in &self.views_schemas {
            for (field_position, field) in table.value().fields().iter().enumerate() {
                builder.add_column(
                    &self.catalog_name,
                    INFORMATION_SCHEMA,
                    table.key(),
                    TableType::View,
                    field_position,
                    field,
                );
            }
        }
        Ok(())
    }

    pub(crate) fn make_databases(
        &self,
        builder: &mut InformationSchemaDatabasesBuilder,
    ) -> datafusion_common::Result<(), DataFusionError> {
        for catalog_name in self.catalog_list.catalog_names() {
            let catalog = self.catalog_list.catalog(&catalog_name).ok_or_else(|| {
                df_error::CatalogNotFoundSnafu {
                    name: catalog_name.clone(),
                }
                .build()
            })?;
            if let Some(caching_catalog) = catalog.as_any().downcast_ref::<CachingCatalog>() {
                let (created_at, updated_at) =
                    if let Some(props) = caching_catalog.properties.clone() {
                        (
                            Some(props.created_at.timestamp_millis()),
                            Some(props.updated_at.timestamp_millis()),
                        )
                    } else {
                        (None, None)
                    };
                builder.add_database(
                    &catalog_name,
                    "",
                    caching_catalog.catalog_type.to_string(),
                    created_at,
                    updated_at,
                );
            } else {
                builder.add_database(&catalog_name, "", "", None, None);
            }
        }
        Ok(())
    }

    /// Construct the `information_schema.df_settings` virtual table
    pub(crate) fn make_df_settings(
        config_options: &ConfigOptions,
        builder: &mut InformationSchemaDfSettingsBuilder,
    ) {
        let standard = ConfigVisitor::standard_entries(config_options);
        for entry in standard {
            builder.add_setting(
                entry.key,
                entry.value.unwrap_or_default(),
                "text",
                entry.description,
                None,
                None,
                None,
            );
        }

        let config = config_options.extensions.get::<SessionParams>();
        if let Some(cfg) = config {
            for entry in cfg.properties.iter() {
                let key = entry.key();
                let prop = entry.value();

                builder.add_setting(
                    key,
                    prop.value.clone(),
                    prop.property_type.clone(),
                    prop.comment.clone().unwrap_or_default(),
                    Some(prop.created_on.to_string()),
                    Some(prop.updated_on.to_string()),
                    prop.session_id.clone(),
                );
            }
        }
    }

    pub(crate) fn make_routines(
        udfs: &HashMap<String, Arc<ScalarUDF>>,
        udafs: &HashMap<String, Arc<AggregateUDF>>,
        udwfs: &HashMap<String, Arc<WindowUDF>>,
        config_options: &ConfigOptions,
        builder: &mut InformationSchemaRoutinesBuilder,
    ) {
        let catalog_name = &config_options.catalog.default_catalog;
        let schema_name = &config_options.catalog.default_schema;

        let mut add_routines = |name: &str,
                                return_types: Vec<Option<String>>,
                                routine_type: &str,
                                is_deterministic: bool,
                                doc: Option<&Documentation>| {
            let description = doc.map(|d| d.description.to_string());
            let example = doc.map(|d| d.syntax_example.to_string());

            for return_type in return_types {
                builder.add_routine(
                    catalog_name,
                    schema_name,
                    name,
                    "FUNCTION",
                    is_deterministic,
                    return_type,
                    routine_type,
                    description.clone(),
                    example.clone(),
                );
            }
        };

        for (name, udf) in udfs {
            let return_types = get_udf_args_and_return_types(udf)
                .into_iter()
                .map(|(_, ret)| ret)
                .collect();
            add_routines(
                name,
                return_types,
                "SCALAR",
                Self::is_deterministic(udf.signature()),
                udf.documentation(),
            );
        }

        for (name, udaf) in udafs {
            let return_types = get_udaf_args_and_return_types(udaf)
                .into_iter()
                .map(|(_, ret)| ret)
                .collect();
            add_routines(
                name,
                return_types,
                "AGGREGATE",
                Self::is_deterministic(udaf.signature()),
                udaf.documentation(),
            );
        }

        for (name, udwf) in udwfs {
            let return_types = get_udwf_args_and_return_types(udwf)
                .into_iter()
                .map(|(_, ret)| ret)
                .collect();
            add_routines(
                name,
                return_types,
                "WINDOW",
                Self::is_deterministic(udwf.signature()),
                udwf.documentation(),
            );
        }
    }

    fn is_deterministic(signature: &Signature) -> bool {
        signature.volatility == Volatility::Immutable
    }
    pub(crate) fn make_parameters(
        udfs: &HashMap<String, Arc<ScalarUDF>>,
        udafs: &HashMap<String, Arc<AggregateUDF>>,
        udwfs: &HashMap<String, Arc<WindowUDF>>,
        config_options: &ConfigOptions,
        builder: &mut InformationSchemaParametersBuilder,
    ) -> datafusion_common::Result<()> {
        let catalog_name = &config_options.catalog.default_catalog;
        let schema_name = &config_options.catalog.default_schema;
        let mut add_parameters = |func_name: &str,
                                  args: Option<&Vec<(String, String)>>,
                                  arg_types: Vec<String>,
                                  return_type: Option<String>,
                                  is_variadic: bool,
                                  rid: usize|
         -> datafusion_common::Result<()> {
            let rid = u8::try_from(rid).context(df_error::RidParamDoesntFitInU8Snafu)?;
            for (position, type_name) in arg_types.iter().enumerate() {
                let param_name = args.and_then(|a| a.get(position).map(|arg| arg.0.as_str()));
                let ordinal_position = u64::try_from(position + 1)
                    .context(df_error::OrdinalPositionParamOverflowSnafu)?;

                builder.add_parameter(
                    catalog_name,
                    schema_name,
                    func_name,
                    ordinal_position,
                    "IN",
                    param_name.map(String::from),
                    type_name,
                    None,
                    is_variadic,
                    rid,
                );
            }
            if let Some(return_type) = return_type {
                builder.add_parameter(
                    catalog_name,
                    schema_name,
                    func_name,
                    1,
                    "OUT",
                    None,
                    return_type.as_str(),
                    None,
                    false,
                    rid,
                );
            }
            Ok(())
        };

        for (func_name, udf) in udfs {
            let args = udf.documentation().and_then(|d| d.arguments.clone());
            let combinations = get_udf_args_and_return_types(udf);
            for (rid, (arg_types, return_type)) in combinations.into_iter().enumerate() {
                add_parameters(
                    func_name,
                    args.as_ref(),
                    arg_types,
                    return_type,
                    Self::is_variadic(udf.signature()),
                    rid,
                )?;
            }
        }

        for (func_name, udaf) in udafs {
            let args = udaf.documentation().and_then(|d| d.arguments.clone());
            let combinations = get_udaf_args_and_return_types(udaf);
            for (rid, (arg_types, return_type)) in combinations.into_iter().enumerate() {
                add_parameters(
                    func_name,
                    args.as_ref(),
                    arg_types,
                    return_type,
                    Self::is_variadic(udaf.signature()),
                    rid,
                )?;
            }
        }

        for (func_name, udwf) in udwfs {
            let args = udwf.documentation().and_then(|d| d.arguments.clone());
            let combinations = get_udwf_args_and_return_types(udwf);
            for (rid, (arg_types, return_type)) in combinations.into_iter().enumerate() {
                add_parameters(
                    func_name,
                    args.as_ref(),
                    arg_types,
                    return_type,
                    Self::is_variadic(udwf.signature()),
                    rid,
                )?;
            }
        }

        Ok(())
    }

    const fn is_variadic(signature: &Signature) -> bool {
        matches!(
            signature.type_signature,
            TypeSignature::Variadic(_) | TypeSignature::VariadicAny
        )
    }
}
