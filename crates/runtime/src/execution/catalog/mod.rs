

use std::{any::Any, collections::{HashMap, HashSet}, sync::Arc};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use chrono::DateTime;
use datafusion::{arrow::datatypes::Field, logical_expr::TableType, catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, Session, TableProvider}, datasource::{file_format::parquet::ParquetFormat, listing::PartitionedFile, physical_plan::FileScanConfig}, execution::object_store::ObjectStoreUrl, physical_expr::create_physical_expr, physical_optimizer::pruning::PruningPredicate};
use datafusion_common::{exec_err, not_impl_err, plan_err, DataFusionError, Result as DFResult, ScalarValue};
use datafusion_expr::{Expr, TableProviderFilterPushDown};
use datafusion_expr::{dml::InsertOp, utils::conjunction, JoinType};
use datafusion_iceberg::{DataFusionTable as IcebergDataFusionTable, pruning_statistics::{PruneDataFiles, PruneManifests}, statistics::manifest_statistics};
use datafusion_physical_plan::{insert::DataSinkExec, ExecutionPlan};
use iceberg_rust::{catalog::{commit::{CommitTable as IcebergCommitTable, CommitView as IcebergCommitView}, create::{CreateMaterializedView as IcebergCreateMaterializedView, CreateTable as IcebergCreateTable, CreateView as IcebergCreateView}, tabular::Tabular as IcebergTabular, Catalog as IcebergCatalog}, error::Error as IcebergError, materialized_view::MaterializedView as IcebergMaterializedView, object_store::Bucket as IcebergBucket, spec::{identifier::Identifier as IcebergIdentifier, manifest::ManifestEntry, manifest_list::ManifestListEntry}, table::Table as IcebergTable, view::View as IcebergView};
use iceberg_rust_spec::{identifier::FullIdentifier as IcebergFullIdentifier, manifest::{Content, Status}, namespace::Namespace as IcebergNamespace, schema::Schema, types::{StructField, StructType}, util};
use icebucket_metastore::{IceBucketSchema, IceBucketSchemaIdent, IceBucketTable, IceBucketTableIdent, Metastore};
use object_store::{ObjectMeta, ObjectStore};

use iceberg_rust_spec::arrow::schema::*;

#[derive(Clone)]
pub struct IceBucketDFMetastore {
    pub metastore: Arc<dyn Metastore>,
}

impl IceBucketDFMetastore {
    pub fn new(metastore: Arc<dyn Metastore>) -> Self {
        IceBucketDFMetastore {
            metastore,
        }
    }
}

 // TODO: Fix iceberg_rust so that the Table struct is a trait
// because the Iceberg Catalog .object_store method expects
// a 'Bucket' type which is very limiting

impl std::fmt::Debug for IceBucketDFMetastore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IceBucketDFMetastore")
            .finish()
    }
}

// Explore using AsyncCatalogProviderList alongside CatalogProviderList
impl CatalogProviderList for IceBucketDFMetastore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn datafusion::catalog::CatalogProvider>,
    ) -> Option<Arc<dyn datafusion::catalog::CatalogProvider>> {
        // This is currently a NOOP because we don't support registering new catalogs yet
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        // TODO: Cache the catalog names in the metastore to avoid async calls
        tokio::runtime::Handle::current()
            .block_on(async {
                self.metastore.list_databases()
                    .await
                    .unwrap_or_default()
                    .iter()
                    .map(|catalog| catalog.ident.clone())
                    .collect()
            })
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::CatalogProvider>> {
        let database = tokio::runtime::Handle::current()
            .block_on(async {
                self.metastore.get_database(&name.to_string())
                    .await
                    .unwrap_or_default()
            });
        database.map(|database| {
            Arc::new(IceBucketDFCatalog {
                ident: database.ident.clone(),
                metastore: self.metastore.clone(),
            }) as Arc<dyn datafusion::catalog::CatalogProvider>
        })
    }
}

pub struct IceBucketDFCatalog {
    pub ident: String,
    pub metastore: Arc<dyn Metastore>,
}

impl std::fmt::Debug for IceBucketDFCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IceBucketDFCatalog")
            .field("ident", &self.ident)
            .finish()
    }
}

impl CatalogProvider for IceBucketDFCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        tokio::runtime::Handle::current()
            .block_on(async {
                self.metastore.list_schemas(&self.ident)
                    .await
                    .unwrap_or_default()
                    .iter()
                    .map(|schema| schema.ident.schema.clone())
                    .collect()
            })
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::SchemaProvider>> {
        let schema = tokio::runtime::Handle::current()
            .block_on(async {
                self.metastore.get_schema(&IceBucketSchemaIdent {
                    database: self.ident.clone(),
                    schema: name.to_string(),
                })
                    .await
                    .unwrap_or_default()
            });
        schema.map(|schema| {
            Arc::new(IceBucketDFSchema {
                database: schema.ident.database.clone(),
                schema: schema.ident.schema.clone(),
                metastore: self.metastore.clone(),
            }) as Arc<dyn datafusion::catalog::SchemaProvider>
        })
    }
}

pub struct IceBucketDFSchema {
    pub database: String,
    pub schema: String,
    pub metastore: Arc<dyn Metastore>,
}

impl std::fmt::Debug for IceBucketDFSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IceBucketDFSchema")
            .field("database", &self.database)
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait]
impl datafusion::catalog::SchemaProvider for IceBucketDFSchema {
    /// Returns the owner of the Schema, default is None. This value is reported
    /// as part of `information_tables.schemata
    fn owner_name(&self) -> Option<&str> {
        None
    }

    /// Returns this `SchemaProvider` as [`Any`] so that it can be downcast to a
    /// specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String> {
        tokio::runtime::Handle::current()
            .block_on(async {
                self.metastore.list_tables(&IceBucketSchemaIdent { schema: self.schema.clone(), database: self.database.clone() })
                    .await
                    .unwrap_or_default()
                    .iter()
                    .map(|table| table.ident.table.clone())
                    .collect()
            })
    }

    /// Retrieves a specific table from the schema by name, if it exists,
    /// otherwise returns `None`.
    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let table_ident = IceBucketTableIdent {
            schema: self.schema.clone(),
            database: self.database.clone(),
            table: name.to_string(),
        };
        let ident_clone = table_ident.clone();
        let table_object_store = tokio::runtime::Handle::current()
            .block_on(async move {
                self.metastore.table_object_store(&table_ident).await
            })
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        
        if let Some(object_store) = table_object_store {

            let bridge = Arc::new(IceBucketIcebergBridge {
                metastore: self.metastore.clone(),
                ident: ident_clone,
                object_store: object_store.clone(),
            });

            let ib_identifier = IcebergIdentifier::new(&[self.database.clone(), self.schema.clone()], name);
            let tabular = bridge.load_tabular(&ib_identifier).await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let dftable = IcebergDataFusionTable::new(tabular, None, None, None);
            Ok(
                Some(Arc::new(dftable) as Arc<dyn TableProvider>)
            )
        } else {
            Ok(None)
        }
    }

    /// If supported by the implementation, adds a new table named `name` to
    /// this schema.
    ///
    /// If a table of the same name was already registered, returns "Table
    /// already exists" error.
    #[allow(unused_variables)]
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        exec_err!("schema provider does not support registering tables")
    }

    /// If supported by the implementation, removes the `name` table from this
    /// schema and returns the previously registered [`TableProvider`], if any.
    ///
    /// If no `name` table exists, returns Ok(None).
    #[allow(unused_variables)]
    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        exec_err!("schema provider does not support deregistering tables")
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool {
        let table_ident = IceBucketTableIdent {
            schema: self.schema.clone(),
            database: self.database.clone(),
            table: name.to_string(),
        };
        tokio::runtime::Handle::current()
            .block_on(async {
                self.metastore.get_table(&table_ident)
                    .await
                    .unwrap_or_default()
                    .is_some()
            })
    }
    
}

#[derive(Debug)]
pub struct IceBucketIcebergBridge {
    pub metastore: Arc<dyn Metastore>,
    pub ident: IceBucketTableIdent,
    pub object_store: Arc<dyn ObjectStore>,
}

#[async_trait]
impl IcebergCatalog for IceBucketIcebergBridge {
    /// Name of the catalog
    fn name(&self) -> &str {
        &self.ident.database
    }

    /// Create a namespace in the catalog
    async fn create_namespace(
        &self,
        namespace: &IcebergNamespace,
        properties: Option<HashMap<String, String>>,
    ) -> Result<HashMap<String, String>, IcebergError> {
        if namespace.len() > 1 {
            return Err(IcebergError::NotSupported("Nested namespaces are not supported".to_string()));
        }
        let schema_ident = IceBucketSchemaIdent {
            database: self.ident.database.clone(),
            schema: namespace.join("")
        };
        let schema = IceBucketSchema {
            ident: schema_ident.clone(),
            properties: properties.clone(),
        };
        let schema = self.metastore.create_schema(schema_ident, schema)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        Ok(schema.data.properties.unwrap_or_default())
    }

    /// Drop a namespace in the catalog
    async fn drop_namespace(&self, namespace: &IcebergNamespace) -> Result<(), IcebergError> {
        if namespace.len() > 1 {
            return Err(IcebergError::NotSupported("Nested namespaces are not supported".to_string()));
        }
        let schema_ident = IceBucketSchemaIdent {
            database: self.ident.database.clone(),
            schema: namespace.join("")
        };
        self.metastore.delete_schema(&schema_ident, true)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        Ok(())
    }

    /// Load the namespace properties from the catalog
    async fn load_namespace(&self, namespace: &IcebergNamespace)
        -> Result<HashMap<String, String>, IcebergError> {
        if namespace.len() > 1 {
            return Err(IcebergError::NotSupported("Nested namespaces are not supported".to_string()));
        }
        let schema_ident = IceBucketSchemaIdent {
            database: self.ident.database.clone(),
            schema: namespace.join("")
        };
        let schema = self.metastore.get_schema(&schema_ident)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        match schema {
            Some(schema) => Ok(schema.data.properties.unwrap_or_default()),
            None => Err(IcebergError::NotFound(format!("Namespace {}", namespace.join("")))),
        }
    }

    /// Update the namespace properties in the catalog
    async fn update_namespace(
        &self,
        namespace: &IcebergNamespace,
        updates: Option<HashMap<String, String>>,
        removals: Option<Vec<String>>,
    ) -> Result<(), IcebergError> {
        if namespace.len() > 1 {
            return Err(IcebergError::NotSupported("Nested namespaces are not supported".to_string()));
        }
        let schema_ident = IceBucketSchemaIdent {
            database: self.ident.database.clone(),
            schema: namespace.join("")
        };
        let schema = self.metastore.get_schema(&schema_ident)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        match schema {
            Some(schema) => {
                let mut schema = schema.data;
                let mut properties = schema.properties.unwrap_or_default();
                if let Some(updates) = updates {
                    properties.extend(updates);
                }
                if let Some(removals) = removals {
                    for key in removals {
                        properties.remove(&key);
                    }
                }
                schema.properties = Some(properties);
                self.metastore.update_schema(&schema_ident, schema)
                    .await
                    .map_err(|e| IcebergError::External(Box::new(e)))?;
                Ok(())
            }
            None => Err(IcebergError::NotFound(format!("Namespace {}", namespace.join("")))),
        }
    }


    /// Check if a namespace exists
    async fn namespace_exists(&self, namespace: &IcebergNamespace) -> Result<bool, IcebergError> {
        if namespace.len() > 1 {
            return Err(IcebergError::NotSupported("Nested namespaces are not supported".to_string()));
        }
        let schema_ident = IceBucketSchemaIdent {
            database: self.ident.database.clone(),
            schema: namespace.join("")
        };
        Ok(self.metastore.get_schema(&schema_ident)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?
            .is_some())
    }


    /// Lists all tables in the given namespace.
    async fn list_tabulars(&self, namespace: &IcebergNamespace) -> Result<Vec<IcebergIdentifier>, IcebergError> {
        if namespace.len() > 1 {
            return Err(IcebergError::NotSupported("Nested namespaces are not supported".to_string()));
        }
        let schema_ident = IceBucketSchemaIdent {
            database: self.ident.database.clone(),
            schema: namespace.join("")
        };
        Ok(self.metastore.list_tables(&schema_ident)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?
            .iter()
            .map(|table| {
                IcebergIdentifier::new(
                    &[table.ident.database.clone(), table.ident.schema.clone()], 
                    &table.ident.table
                )
            })
            .collect())
    }

    /// Lists all namespaces in the catalog.
    async fn list_namespaces(&self, _parent: Option<&str>) -> Result<Vec<IcebergNamespace>, IcebergError> {
        let mut namespaces = Vec::new();
        let databases = self.metastore.list_databases().await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        for database in databases {
            let schemas = self.metastore.list_schemas(&database.ident).await
                .map_err(|e| IcebergError::External(Box::new(e)))?;
            for schema in schemas {
                namespaces.push(IcebergNamespace::try_new(&[schema.ident.database.clone(), schema.ident.schema.clone()])?);
            }
        }
        Ok(namespaces)
    }

    /// Check if a table exists
    async fn tabular_exists(&self, identifier: &IcebergIdentifier) -> Result<bool, IcebergError> {
        let table_ident = IceBucketTableIdent {
            database: identifier.namespace()[0].clone(),
            schema: identifier.namespace()[1].clone(),
            table: identifier.name().to_string(),
        };
        Ok(self.metastore.get_table(&table_ident)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?
            .is_some())
    }

    /// Drop a table and delete all data and metadata files.
    async fn drop_table(&self, identifier: &IcebergIdentifier) -> Result<(), IcebergError> {
        let table_ident = IceBucketTableIdent {
            database: identifier.namespace()[0].clone(),
            schema: identifier.namespace()[1].clone(),
            table: identifier.name().to_string(),
        };
        self.metastore.delete_table(&table_ident, true)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        Ok(())
    }

    /// Drop a table and delete all data and metadata files.
    async fn drop_view(&self, _identifier: &IcebergIdentifier) -> Result<(), IcebergError> {
        Err(IcebergError::NotSupported("Views are not supported".to_string()))
    }

    /// Drop a table and delete all data and metadata files.
    async fn drop_materialized_view(&self, _identifier: &IcebergIdentifier) -> Result<(), IcebergError> {
        Err(IcebergError::NotSupported("Materialized views are not supported".to_string()))
    }

    /// Load a table.
    async fn load_tabular(self: Arc<Self>, identifier: &IcebergIdentifier) -> Result<IcebergTabular, IcebergError> {
        let table_ident = IceBucketTableIdent {
            database: identifier.namespace()[0].clone(),
            schema: identifier.namespace()[1].clone(),
            table: identifier.name().to_string(),
        };
        let table = self.metastore.get_table(&table_ident)
            .await
            .map_err(|e| IcebergError::External(Box::new(e)))?;
        match table {
            Some(table) => {

                let iceberg_table = IcebergTable::new(
                    identifier.clone(),
                    self.clone(),
                    table.metadata.clone(),
                ).await?;

                Ok(IcebergTabular::Table(iceberg_table))
            },
            None => Err(IcebergError::NotFound(format!("Table {} not found", identifier.name()))),
        }
    }

    /// Create a table in the catalog if it doesn't exist.
    async fn create_table(
        self: Arc<Self>,
        identifier: IcebergIdentifier,
        create_table: IcebergCreateTable,
    ) -> Result<IcebergTable, IcebergError> {
        todo!()
    }

    /// Create a view with the catalog if it doesn't exist.
    async fn create_view(
        self: Arc<Self>,
        _identifier: IcebergIdentifier,
        _create_view: IcebergCreateView<Option<()>>,
    ) -> Result<IcebergView, IcebergError> {
        Err(IcebergError::NotSupported("Views are not supported".to_string()))
    }

    /// Register a materialized view with the catalog if it doesn't exist.
    async fn create_materialized_view(
        self: Arc<Self>,
        _identifier: IcebergIdentifier,
        _create_view: IcebergCreateMaterializedView,
    ) -> Result<IcebergMaterializedView, IcebergError> {
        Err(IcebergError::NotSupported("Materialized views are not supported".to_string()))
    }

    /// perform commit table operation
    async fn update_table(self: Arc<Self>, commit: IcebergCommitTable) -> Result<IcebergTable, IcebergError> {
        todo!()
    }

    /// perform commit view operation
    async fn update_view(self: Arc<Self>, commit: IcebergCommitView<Option<()>>) -> Result<IcebergView, IcebergError> {
        Err(IcebergError::NotSupported("Views are not supported".to_string()))
    }

    /// perform commit view operation
    async fn update_materialized_view(
        self: Arc<Self>,
        _commit: IcebergCommitView<IcebergFullIdentifier>,
    ) -> Result<IcebergMaterializedView, IcebergError> {
        Err(IcebergError::NotSupported("Materialized views are not supported".to_string()))
    }

    /// Register a table with the catalog if it doesn't exist.
    async fn register_table(
        self: Arc<Self>,
        _identifier: IcebergIdentifier,
        _metadata_location: &str,
    ) -> Result<IcebergTable, IcebergError> {
        todo!()
    }

    /// Return the associated object store for a bucket
    fn object_store(&self, _bucket: IcebergBucket) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }
}

/*#[derive(Debug)]
pub struct IceBucketDFTable {
    pub table: IceBucketTable,
    pub metastore: Arc<dyn Metastore>,
}

impl IceBucketDFTable {
    pub async fn new(
        table: IceBucketTable,
        metastore: Arc<dyn Metastore>,
    ) -> Self {
        IceBucketDFTable {
            table,
            metastore,
        }
    }
}

#[async_trait]
impl TableProvider for IceBucketDFTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        let arrow_schema:arrow_schema::Schema = self.table.metadata.current_schema().as_struct().try_into().unwrap();
        Arc::new(arrow_schema)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let statistics = self
                    .statistics();
        
        // TODO: Support snapshot ranges
        let snapshot_range = (None, None);
        let schema = self.table.metadata.current_schema().clone();
        /*let schema = snapshot_range
            .1
            .and_then(|snapshot_id| self.table.metadata.schema_by_id(snapshot_id).ok().cloned())
            .unwrap_or_else(|| self.table.metadata.current_schema().clone());*/

        // Create a unique URI for this particular object store
        let object_store_url = ObjectStoreUrl::parse(&self.table.metadata.location)?;
        let table_object_store = self.metastore.table_object_store(&self.table.ident).await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        state
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), table_object_store);

        // All files have to be grouped according to their partition values. This is done by using a HashMap with the partition values as the key.
        // This way data files with the same partition value are mapped to the same vector.
        let mut data_file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();
        let mut equality_delete_file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> =
            HashMap::new();

        // TODO: Implement when we support snapshot ranges
        /*let partition_fields = &snapshot_range
            .1
            .and_then(|snapshot_id| self.table.metadata.partition_fields(snapshot_id).ok())
            .unwrap_or_else(|| self.table.metadata.current_partition_fields(None).unwrap());*/
        let partition_fields = self.table.metadata.default_partition_spec().fields();

        let partition_column_names = partition_fields
            .iter()
            .filter_map(|field| schema.field_by_id(field.source_id).map(|f| f.name.clone()))         
            .collect::<HashSet<_>>();

        let iceberg_table = IceBucketIcebergTable::new(self.table.clone(), self.metastore.clone()).await;

        // If there is a filter expression the manifests to read are pruned based on the pruning statistics available in the manifest_list file.
        let physical_predicate = if let Some(predicate) = conjunction(filters.iter().cloned()) {
            Some(create_physical_expr(
                &predicate,
                &schema.as_ref().clone().try_into()?,
                state.execution_props(),
            )?)
        } else {
            None
        };
        if let Some(physical_predicate) = physical_predicate.clone() {
            let partition_predicates = conjunction(
                filters
                    .iter()
                    .filter(|expr| {
                        let set: HashSet<String> = expr
                            .column_refs()
                            .into_iter()
                            .map(|x| x.name.clone())
                            .collect();
                        set.is_subset(&partition_column_names)
                    })
                    .cloned(),
            );

            /*let manifests = self.table
                .metadata
                .manifests(snapshot_range.0, snapshot_range.1)
                .await
                .map_err(Into::<Error>::into)
                .map_err(DataFusionIcebergError::from)?;*/



            let manifests:Vec<ManifestListEntry> = iceberg_table.manifests(snapshot_range.0, snapshot_range.1)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // If there is a filter expression on the partition column, the manifest files to read are pruned.
            let data_files: Vec<ManifestEntry> = if let Some(predicate) = partition_predicates {
                let physical_partition_predicate = create_physical_expr(
                    &predicate,
                    &schema.as_ref().clone().try_into()?,
                    state.execution_props(),
                )?;
                let pruning_predicate =
                    PruningPredicate::try_new(physical_partition_predicate, schema.clone())?;
                let manifests_to_prune =
                    pruning_predicate.prune(&PruneManifests::new(partition_fields, &manifests))?;

                // TODO: Implement when we support snapshot ranges
                iceberg_table
                    .datafiles(&manifests, Some(manifests_to_prune), (None, None))
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .try_collect()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            } else {
                iceberg_table
                    .datafiles(&manifests, None, (None, None))
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .try_collect()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            };

            let pruning_predicate =
                PruningPredicate::try_new(physical_predicate, schema.clone())?;
            // After the first pruning stage the data_files are pruned again based on the pruning statistics in the manifest files.
            let files_to_prune =
                pruning_predicate.prune(&PruneDataFiles::new(&schema, &schema, &data_files))?;

            data_files
                .into_iter()
                .zip(files_to_prune.into_iter())
                .for_each(|(manifest, prune_file)| {
                    if prune_file && *manifest.status() != Status::Deleted {
                        let partition_values = manifest
                            .data_file()
                            .partition()
                            .iter()
                            .map(|value| match value {
                                Some(v) => ScalarValue::Utf8(Some(serde_json::to_string(v).unwrap())),
                                None => ScalarValue::Null,
                            })
                            .collect::<Vec<ScalarValue>>();
                        let object_meta = ObjectMeta {
                            location: util::strip_prefix(manifest.data_file().file_path()).into(),
                            size: *manifest.data_file().file_size_in_bytes() as usize,
                            last_modified: {
                                let last_updated_ms = self.table.metadata.last_updated_ms;
                                let secs = last_updated_ms / 1000;
                                let nsecs = (last_updated_ms % 1000) as u32 * 1000000;
                                DateTime::from_timestamp(secs, nsecs).unwrap()
                            },
                            e_tag: None,
                            version: None,
                        };
                        let manifest_statistics = manifest_statistics(&schema, &manifest);
                        let file = PartitionedFile {
                            object_meta,
                            partition_values,
                            range: None,
                            statistics: Some(manifest_statistics),
                            extensions: None,
                            metadata_size_hint: None
                        };
                        match manifest.data_file().content() {
                            Content::Data => {
                                data_file_groups
                                    .entry(file.partition_values.clone())
                                    .or_default()
                                    .push(file);
                            }
                            Content::EqualityDeletes => {
                                equality_delete_file_groups
                                    .entry(file.partition_values.clone())
                                    .or_default()
                                    .push(file);
                            }
                            Content::PositionDeletes => {
                                panic!("Position deletes not supported.")
                            }
                        }
                    };
                });
        } else {
            let manifests = iceberg_table
                .manifests(snapshot_range.0, snapshot_range.1)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            // TODO Implement when we support snapshot ranges
            let data_files: Vec<ManifestEntry> = iceberg_table
                .datafiles(&manifests, None, (None, None))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .try_collect()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            data_files.into_iter().for_each(|manifest| {
                if *manifest.status() != Status::Deleted {
                    let partition_values = manifest
                        .data_file()
                        .partition()
                        .iter()
                        .map(|value| match value {
                            Some(v) => ScalarValue::Utf8(Some(serde_json::to_string(v).unwrap())),
                            None => ScalarValue::Null,
                        })
                        .collect::<Vec<ScalarValue>>();
                    let object_meta = ObjectMeta {
                        location: util::strip_prefix(manifest.data_file().file_path()).into(),
                        size: *manifest.data_file().file_size_in_bytes() as usize,
                        last_modified: {
                            let last_updated_ms = self.table.metadata.last_updated_ms;
                            let secs = last_updated_ms / 1000;
                            let nsecs = (last_updated_ms % 1000) as u32 * 1000000;
                            DateTime::from_timestamp(secs, nsecs).unwrap()
                        },
                        e_tag: None,
                        version: None,
                    };
                    let manifest_statistics = manifest_statistics(&schema, &manifest);
                    let file = PartitionedFile {
                        object_meta,
                        partition_values,
                        range: None,
                        statistics: Some(manifest_statistics),
                        extensions: None,
                        metadata_size_hint: None
                    };
                    match manifest.data_file().content() {
                        Content::Data => {
                            data_file_groups
                                .entry(file.partition_values.clone())
                                .or_default()
                                .push(file);
                        }
                        Content::EqualityDeletes => {
                            equality_delete_file_groups
                                .entry(file.partition_values.clone())
                                .or_default()
                                .push(file);
                        }
                        Content::PositionDeletes => {
                            panic!("Position deletes not supported.")
                        }
                    }
                }
            });
        };

        // Get all partition columns
        let table_partition_cols: Vec<Field> = partition_fields
            .iter()
            .map(|partition_field| {
                Ok(Field::new(
                    partition_field.name().to_owned() + "__partition",
                    (&partition_field
                        .field_type()
                        .tranform(partition_field.transform())
                        .map_err(|e| DataFusionError::External(Box::new(e)))?)
                        .try_into()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                    !partition_field.required(),
                ))
            })
            .collect::<Result<Vec<_>, DataFusionError>>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Add the partition columns to the table schema
        let mut schema_builder = StructType::builder();
        for field in schema.fields().iter() {
            schema_builder.with_struct_field(field.clone());
        }
        for partition_field in partition_fields {
            schema_builder.with_struct_field(StructField {
                id: partition_field.field_id(),
                name: partition_field.name().to_owned() + "__partition",
                field_type: partition_field
                    .field_type()
                    .tranform(partition_field.transform())
                    .unwrap(),
                required: true,
                doc: None,
            });
        }
        let file_schema = Schema::builder()
            .with_schema_id(*schema.schema_id())
            .with_fields(
                schema_builder
                    .build()
                    .map_err(iceberg_rust::spec::error::Error::from)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            )
            .build()
            .map_err(iceberg_rust::spec::error::Error::from)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let file_schema: SchemaRef = Arc::new((file_schema.fields()).try_into().unwrap());

        let file_scan_config = FileScanConfig {
            object_store_url,
            file_schema,
            file_groups: data_file_groups.into_values().collect(),
            constraints: Default::default(),
            statistics,
            projection: projection.cloned(),
            limit,
            table_partition_cols,
            output_ordering: vec![],
        };

        ParquetFormat::default()
            .create_physical_plan(state, file_scan_config, physical_predicate.as_ref())
            .await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Create a physical plan from the logical plan.
        // Check that the schema of the plan matches the schema of this table.
        if !self.schema().equivalent_names_and_types(&input.schema()) {
            return plan_err!("Inserting query must have the same schema with the table.");
        }
        let InsertOp::Append = insert_op else {
            return not_impl_err!("Overwrite not implemented for MemoryTable yet");
        };
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(self.clone().into_data_sink()),
            None,
        )))
    }

    fn statistics(&self) -> Option<datafusion::physical_plan::Statistics> {
        None
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}*/