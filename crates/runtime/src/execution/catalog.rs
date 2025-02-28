use std::{any::Any, collections::{HashMap, HashSet}, sync::Arc};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, Session, TableProvider}, datasource::listing::PartitionedFile, execution::object_store::ObjectStoreUrl};
use datafusion_common::{exec_err, DataFusionError, Result as DFResult, ScalarValue};
use datafusion_expr::Expr;
use datafusion_iceberg::DataFusionTable as IcebergDataFusionTable;
use datafusion_physical_plan::ExecutionPlan;
use iceberg_rust::{spec::identifier::Identifier, table::{self, Table as IcebergTable}, error::Error as IcebergError};
use icebucket_metastore::{IceBucketSchemaIdent, IceBucketTable, IceBucketTableIdent, Metastore};


pub struct IceBucketDFMetastore {
    pub metastore: Arc<dyn Metastore>,
}

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
        let table = tokio::runtime::Handle::current()
            .block_on(async move {
                self.metastore.get_table(&table_ident).await
            })
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        
        Ok(table.map(|table| {
            Arc::new(IceBucketDFTable::new(ident_clone, table.data, self.metastore.clone(), false)) as Arc<dyn TableProvider>
        }))
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
pub struct IceBucketDFTable {
    pub table_ident: IceBucketTableIdent,
    pub table: IceBucketTable,
    pub metastore: Arc<dyn Metastore>,
}

impl IceBucketDFTable {
    pub async fn new(
        table_ident: IceBucketTableIdent,
        ib_table: IceBucketTable,
        metastore: Arc<dyn Metastore>,
    ) -> Self {
        IceBucketDFTable {
            table_ident,
            table: ib_table,
            metastore,
        }
    }
}

#[async_trait]
impl TableProvider for IceBucketDFTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        //TODO: Replace some of these with actual values
        /*table: &Table,
        snapshot_range: &(Option<i64>, Option<i64>),
        arrow_schema: SchemaRef,
        statistics: Statistics,
        session: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,*/
        
        // TODO: Support snapshot ranges
        let snapshot_range = (None, None);
        let schema = self.table.metadata.current_schema().clone();
        /*let schema = snapshot_range
            .1
            .and_then(|snapshot_id| self.table.metadata.schema_by_id(snapshot_id).ok().cloned())
            .unwrap_or_else(|| self.table.metadata.current_schema().clone());*/

        // Create a unique URI for this particular object store
        let object_store_url = ObjectStoreUrl::parse(&self.table.metadata.location)?;
        let table_object_store = self.metastore.table_object_store(&self.table_ident).await
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
        

        // If there is a filter expression the manifests to read are pruned based on the pruning statistics available in the manifest_list file.
        let physical_predicate = if let Some(predicate) = conjunction(filters.iter().cloned()) {
            Some(create_physical_expr(
                &predicate,
                &arrow_schema.as_ref().clone().try_into()?,
                session.execution_props(),
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

            let manifests = table
                .manifests(snapshot_range.0, snapshot_range.1)
                .await
                .map_err(Into::<Error>::into)
                .map_err(DataFusionIcebergError::from)?;

            // If there is a filter expression on the partition column, the manifest files to read are pruned.
            let data_files: Vec<ManifestEntry> = if let Some(predicate) = partition_predicates {
                let physical_partition_predicate = create_physical_expr(
                    &predicate,
                    &arrow_schema.as_ref().clone().try_into()?,
                    session.execution_props(),
                )?;
                let pruning_predicate =
                    PruningPredicate::try_new(physical_partition_predicate, arrow_schema.clone())?;
                let manifests_to_prune =
                    pruning_predicate.prune(&PruneManifests::new(partition_fields, &manifests))?;

                table
                    .datafiles(&manifests, Some(manifests_to_prune))
                    .await
                    .map_err(DataFusionIcebergError::from)?
                    .try_collect()
                    .await
                    .map_err(DataFusionIcebergError::from)?
            } else {
                table
                    .datafiles(&manifests, None)
                    .await
                    .map_err(DataFusionIcebergError::from)?
                    .try_collect()
                    .await
                    .map_err(DataFusionIcebergError::from)?
            };

            let pruning_predicate =
                PruningPredicate::try_new(physical_predicate, arrow_schema.clone())?;
            // After the first pruning stage the data_files are pruned again based on the pruning statistics in the manifest files.
            let files_to_prune =
                pruning_predicate.prune(&PruneDataFiles::new(&schema, &arrow_schema, &data_files))?;

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
                                let last_updated_ms = table.metadata().last_updated_ms;
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
            let manifests = table
                .manifests(snapshot_range.0, snapshot_range.1)
                .await
                .map_err(DataFusionIcebergError::from)?;
            let data_files: Vec<ManifestEntry> = table
                .datafiles(&manifests, None)
                .await
                .map_err(DataFusionIcebergError::from)?
                .try_collect()
                .await
                .map_err(DataFusionIcebergError::from)?;
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
                            let last_updated_ms = table.metadata().last_updated_ms;
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
                        .map_err(DataFusionIcebergError::from)?)
                        .try_into()
                        .map_err(DataFusionIcebergError::from)?,
                    !partition_field.required(),
                ))
            })
            .collect::<Result<Vec<_>, DataFusionError>>()
            .map_err(DataFusionIcebergError::from)?;

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
                    .map_err(DataFusionIcebergError::from)?,
            )
            .build()
            .map_err(iceberg_rust::spec::error::Error::from)
            .map_err(DataFusionIcebergError::from)?;

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
            .create_physical_plan(session, file_scan_config, physical_predicate.as_ref())
            .await
    }

    fn statistics(&self) -> Option<datafusion::physical_plan::Statistics> {
        todo!()
    }

    fn has_partiotion_keys(&self) -> bool {
        todo!()
    }

    fn partition_keys(&self) -> Vec<String> {
        todo!()
    }

    fn projection(&self) -> Option<Vec<usize>> {
        todo!()
    }

    fn schema_name(&self) -> &str {
        todo!()
    }

    fn table_name(&self) -> &str {
        todo!()
    }

    fn table_type(&self) -> datafusion::catalog::TableType {
        todo!()
    }
}