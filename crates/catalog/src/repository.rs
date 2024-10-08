use async_trait::async_trait;
use iceberg::io::FileIOBuilder;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::StaticTable;
use iceberg::ErrorKind;
use serde_json::de;
use serde_json::ser;
use slatedb::db::Db;
use std::fmt::Formatter;
use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug};

use iceberg::{
    table::Table, Error, Namespace, NamespaceIdent, Result, TableCommit as TableCommitOld,
    TableCreation, TableIdent, TableRequirement, TableUpdate,
};

pub use iceberg::Catalog;
pub use iceberg_catalog_memory::MemoryCatalog as InMemoryCatalogRepository;

use crate::models::TableCommit;
use control_plane::models::Warehouse;

const DBPREFIX: &str = "db";
const SEP: &str = "\u{001f}";
const TBLPREFIX: &str = "tbl";
const ALL: &str = "all";

#[async_trait]
pub trait Repository: Catalog {
    async fn update_table_ext(&self, commit: TableCommit) -> Result<Table>;
}

pub struct DbRepository {
    db: Arc<Db>,
    warehouse: Warehouse,
}

// Repository trait requires Debug trait
impl Debug for DbRepository {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DbRepository")
    }
}

impl DbRepository {
    pub fn new(db: Arc<Db>, warehouse: Warehouse) -> Self {
        Self { db, warehouse }
    }

    async fn put<T: serde::Serialize>(&self, key: &str, value: T) -> Result<()> {
        let serialized = ser::to_vec(&value)
            .map_err(|e| Error::new(ErrorKind::Unexpected, "failed to serialize").with_source(e))?;
        self.db.put(key.as_bytes(), serialized.as_ref()).await;
        Ok(())
    }

    async fn get<T: for<'de> serde::de::Deserialize<'de>>(&self, key: &str) -> Result<Option<T>> {
        let value: Option<bytes::Bytes> =
            self.db.get(key.as_bytes()).await.map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to read db").with_source(e)
            })?;
        value.map_or_else(
            || Ok(None),
            |bytes| {
                de::from_slice(&bytes).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "failed to deserialize value").with_source(e)
                })
            },
        )
    }

    async fn keys(&self, key: &str) -> Result<Vec<String>> {
        let keys: Option<Vec<String>> = self.get(key).await?;
        Ok(keys.unwrap_or_default())
    }

    async fn append(&self, key: String, value: String) -> Result<()> {
        self.modify(key.as_ref(), |all_keys: &mut Vec<String>| {
            all_keys.push(value.clone());
        })
        .await?;
        Ok(())
    }

    async fn remove(&self, key: String, value: String) -> Result<()> {
        self.modify(key.as_ref(), |all_keys: &mut Vec<String>| {
            all_keys.retain(|key| *key != value);
        })
        .await?;
        Ok(())
    }

    // function that takes closure as argument
    // it reads value from the db, deserialize it and pass it to the closure
    // it then gets value from the clousre, serialize it and write it back to the db
    async fn modify<T>(&self, key: &str, f: impl Fn(&mut T)) -> Result<()>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Default,
    {
        let mut value: T = self.get(key).await?.unwrap_or_default();

        f(&mut value);

        self.put(key, value).await?;

        Ok(())
    }
}

#[async_trait]
impl Catalog for DbRepository {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let warehouse = self.warehouse.id;
        let key = format!("{DBPREFIX}.{warehouse}.{ALL}");

        let keys = self.keys(&key).await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to list namespaces").with_source(e)
        })?;

        Ok(keys
            .into_iter()
            .map(|s| NamespaceIdent::from_strs(s.split(SEP).collect::<Vec<_>>()))
            .collect::<Result<Vec<NamespaceIdent>>>()?)
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let warehouse = self.warehouse.id;
        let name = namespace.to_url_string();
        let key = format!("{DBPREFIX}.{warehouse}.{name}");

        self.put(&key, properties.clone()).await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to create namespace").with_source(e)
        })?;

        self.append(format!("{DBPREFIX}.{warehouse}.{ALL}"), name)
            .await
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to create namespace").with_source(e)
            })?;

        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let warehouse = self.warehouse.id;
        let name = namespace.to_url_string();
        let key = format!("{DBPREFIX}.{warehouse}.{name}");

        let properties = self.get(&key).await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to read namespace").with_source(e)
        })?;
        let properties =
            properties.ok_or_else(|| Error::new(ErrorKind::DataInvalid, "namespace not found"))?;

        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.get_namespace(namespace).await.map(|_| true)
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "update namespace is not supported",
        ))
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let warehouse = self.warehouse.id;
        let name = namespace.to_url_string();
        let key = format!("{DBPREFIX}.{warehouse}.{name}");
        self.db.delete(key.as_bytes()).await;

        self.remove(format!("{DBPREFIX}.{warehouse}.{ALL}"), name)
            .await
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to drop namespace").with_source(e)
            })?;

        Ok(())
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let warehouse = self.warehouse.id;
        let database = namespace.to_url_string();
        let key = format!("{TBLPREFIX}.{warehouse}.{database}.{ALL}");

        let keys = self.keys(&key).await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to list tables").with_source(e)
        })?;

        Ok(keys
            .into_iter()
            .map(|s| TableIdent {
                namespace: namespace.clone(),
                name: s.to_string(),
            })
            .collect())
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let name = creation.name.to_string();
        let metadata = TableMetadataBuilder::from_table_creation(creation)
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to build table metadata").with_source(e)
            })?
            .build()
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to build table metadata").with_source(e)
            })?
            .metadata;

        let warehouse = self.warehouse.id;
        let database = namespace.to_url_string();
        let key = format!("{TBLPREFIX}.{warehouse}.{database}.{name}");

        self.put(&key, metadata.clone()).await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to create table").with_source(e)
        })?;

        self.append(
            format!("{TBLPREFIX}.{warehouse}.{database}.{ALL}"),
            name.clone(),
        )
        .await
        .map_err(|e| Error::new(ErrorKind::Unexpected, "failed to create table").with_source(e))?;

        // TODO: update file io to match actual storage
        // i.e. FileIOBuilder::new(warehouse.location)
        let file_io = FileIOBuilder::new_fs_io().build().map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to build file io").with_source(e)
        })?;

        let table = StaticTable::from_metadata(
            metadata,
            TableIdent {
                namespace: namespace.clone(),
                name,
            },
            file_io,
        )
        .await?
        .into_table();
        Ok(table)
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        let warehouse = self.warehouse.id;
        let database = table.namespace.to_url_string();
        let tblname = table.name.as_str();
        let key = format!("{TBLPREFIX}.{warehouse}.{database}.{tblname}");

        let metadata = self.get(&key).await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to read table").with_source(e)
        })?;
        let metadata =
            metadata.ok_or_else(|| Error::new(ErrorKind::DataInvalid, "table not found"))?;

        let file_io = FileIOBuilder::new_fs_io().build().map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to build file io").with_source(e)
        })?;
        let table = StaticTable::from_metadata(
            metadata,
            TableIdent {
                namespace: table.namespace.clone(),
                name: table.name.clone(),
            },
            file_io,
        )
        .await?
        .into_table();
        Ok(table)
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let warehouse = self.warehouse.id;
        let database = table.namespace.to_url_string();
        let tblname = table.name.as_str();
        let key = format!("{TBLPREFIX}.{warehouse}.{database}.{tblname}");
        self.db.delete(key.as_bytes()).await;

        // Update special key all value
        let key = format!("{TBLPREFIX}.{warehouse}.{database}.{ALL}");
        self.remove(key, tblname.to_string()).await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to drop table").with_source(e)
        })?;

        Ok(())
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        self.load_table(table).await.map(|_| true)
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "rename table is not supported",
        ))
    }

    async fn update_table(&self, _commit: TableCommitOld) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "update table is not supported",
        ))
    }
}

#[async_trait]
impl Repository for DbRepository {
    async fn update_table_ext(&self, commit: TableCommit) -> Result<Table> {
        let warehouse = self.warehouse.id;
        let database = commit.ident.namespace.to_url_string();
        let tblname = commit.ident.name.as_str();
        let key = format!("{TBLPREFIX}.{warehouse}.{database}.{tblname}");

        let table = self.load_table(&commit.ident).await?;

        let commit = commit;

        commit
            .requirements
            .into_iter()
            .map(TableRequirementExt::new)
            .try_for_each(|req| req.assert(table.metadata(), true))?;

        let (metadata, location) = (table.metadata().clone(), table.metadata_location());
        let mut builder =
            TableMetadataBuilder::new_from_metadata(metadata, location.map(Into::into));

        for update in commit.updates.into_iter() {
            builder = update.apply(builder).map_err(|e| {
                Error::new(ErrorKind::DataInvalid, "failed to apply update").with_source(e)
            })?;
        }
        let metadata = builder.build().map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "failed to build metadata").with_source(e)
        })?;

        let value = ser::to_vec(&metadata.metadata).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to serialize metadata").with_source(e)
        })?;

        self.db.put(key.as_bytes(), &value).await;

        let table = StaticTable::from_metadata(
            metadata.metadata,
            TableIdent {
                namespace: commit.ident.namespace.clone(),
                name: commit.ident.name.clone(),
            },
            table.file_io().clone(),
        )
        .await?;

        Ok(table.into_table())
    }
}

pub struct TableRequirementExt(TableRequirement);

impl From<TableRequirement> for TableRequirementExt {
    fn from(requirement: TableRequirement) -> Self {
        Self(requirement)
    }
}

impl TableRequirementExt {
    pub fn new(requirement: TableRequirement) -> Self {
        Self(requirement)
    }

    fn inner(&self) -> &TableRequirement {
        &self.0
    }

    fn assert(&self, metadata: &TableMetadata, exists: bool) -> Result<()> {
        match self.inner() {
            TableRequirement::NotExist => {
                if exists {
                    return Err(Error::new(ErrorKind::DataInvalid, "Table exists"));
                }
            }
            TableRequirement::UuidMatch { uuid } => {
                if &metadata.uuid() != uuid {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Table uuid does not match",
                    ));
                }
            }
            TableRequirement::CurrentSchemaIdMatch { current_schema_id } => {
                // ToDo: Harmonize the types of current_schema_id
                if i64::from(metadata.current_schema_id) != *current_schema_id {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Table current schema id does not match",
                    ));
                }
            }
            TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id,
            } => {
                if metadata.default_sort_order_id != *default_sort_order_id {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Table default sort order id does not match",
                    ));
                }
            }
            TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id } => {
                if let Some(snapshot_id) = snapshot_id {
                    let snapshot_ref = metadata
                        .refs
                        .get(r#ref)
                        .ok_or(Error::new(ErrorKind::DataInvalid, "Table ref not found"))?;
                    if snapshot_ref.snapshot_id != *snapshot_id {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Table ref snapshot id does not match",
                        ));
                    }
                } else if metadata.refs.contains_key(r#ref) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Table ref snapshot id does not match",
                    ));
                }
            }
            TableRequirement::DefaultSpecIdMatch { default_spec_id } => {
                // ToDo: Harmonize the types of default_spec_id
                if i64::from(metadata.default_partition_spec_id()) != *default_spec_id {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Table default spec id does not match",
                    ));
                }
            }
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id,
            } => {
                if i64::from(metadata.last_partition_id) != *last_assigned_partition_id {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Table last assigned partition id does not match",
                    ));
                }
            }
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id,
            } => {
                // ToDo: Harmonize types
                let last_column_id: i64 = metadata.last_column_id.into();
                if &last_column_id != last_assigned_field_id {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Table last assigned field id does not match",
                    ));
                }
            }
        };
        Ok(())
    }
}

mod tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use slatedb::config::DbOptions;
    use slatedb::db::Db;
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_namespace_create() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = DbOptions::default();
        let db = Db::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
            .await
            .unwrap();

        let wh = Warehouse::new("test".to_string(), "test".to_string(), Uuid::new_v4())
            .expect("warehouse creation failed");
        let repo = DbRepository::new(Arc::new(db), wh);

        let namespace = NamespaceIdent::new("test".into());
        let mut properties = HashMap::new();
        properties.insert("key1".to_string(), "value1".to_string());

        let res = repo.create_namespace(&namespace, properties).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let res = repo.list_namespaces(None).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let res = res.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], namespace);

        let res = repo.get_namespace(&namespace).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());
        assert_eq!(res.unwrap().properties().len(), 1);

        let res = repo.drop_namespace(&namespace).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let res = repo.list_namespaces(None).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());
        assert_eq!(res.unwrap().len(), 0);
    }

    async fn create_table(
        repo: &DbRepository,
        namespace: &NamespaceIdent,
        name: Option<&str>,
    ) -> Result<Table> {
        let test_schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();
        let name = name.unwrap_or("test_table");
        let creation = TableCreation::builder()
            .name(name.into())
            .schema(test_schema)
            .location("test_table".into())
            .build();

        repo.create_table(namespace, creation).await
    }

    #[tokio::test]
    async fn test_table_create_and_drop() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = DbOptions::default();
        let db = Db::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
            .await
            .unwrap();

        let wh = Warehouse::new("test".to_string(), "test".to_string(), Uuid::new_v4())
            .expect("warehouse creation failed");
        let repo = DbRepository::new(Arc::new(db), wh);

        let res = create_table(
            &repo,
            &NamespaceIdent::new("test".into()),
            Some(format!("test_table_{}", Uuid::new_v4()).as_str()),
        )
        .await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let ident = res.as_ref().unwrap().identifier();
        let res = repo.drop_table(ident).await;
        assert!(res.is_ok(), "{}", res.unwrap_err());

        let res = repo.list_tables(&NamespaceIdent::new("test".into())).await;
        assert!(res.is_ok(), "{}", res.unwrap_err());

        let res = res.unwrap();
        assert_eq!(res.len(), 0);
    }

    #[tokio::test]
    async fn test_table_create_get() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = DbOptions::default();
        let db = Db::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
            .await
            .unwrap();

        let wh = Warehouse::new("test".to_string(), "test".to_string(), Uuid::new_v4())
            .expect("warehouse creation failed");
        let repo = DbRepository::new(Arc::new(db), wh);

        for i in 0..2 {
            let res = create_table(
                &repo,
                &NamespaceIdent::new("test".into()),
                Some(format!("test_table_{i}").as_str()),
            )
            .await;
            assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

            let ident = res.as_ref().unwrap().identifier();
            let res_load = repo.load_table(ident).await;
            assert!(res_load.is_ok());

            assert_eq!(res_load.unwrap().metadata(), res.unwrap().metadata());
        }

        let res = repo.list_tables(&NamespaceIdent::new("test".into())).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let res = res.unwrap();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].name(), "test_table_0");
        assert_eq!(res[1].name(), "test_table_1");
    }

    #[tokio::test]
    async fn test_update_table() {
        let json = r#"
{
    "action": "add-sort-order",
    "sort-order": {
        "order-id": 1,
        "fields": [
            {
                "transform": "identity",
                "source-id": 3,
                "direction": "asc",
                "null-order": "nulls-first"
            },
            {
                "transform": "bucket[4]",
                "source-id": 2,
                "direction": "desc",
                "null-order": "nulls-last"
            }
        ]
    }
}
        "#;
        let update: TableUpdate = serde_json::from_str(json).unwrap();

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = DbOptions::default();
        let db = Db::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
            .await
            .unwrap();

        let wh = Warehouse::new("test".to_string(), "test".to_string(), Uuid::new_v4())
            .expect("warehouse creation failed");
        let repo = DbRepository::new(Arc::new(db), wh);

        let res = create_table(
            &repo,
            &NamespaceIdent::new("test".into()),
            Some("test_table"),
        )
        .await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let ident = res.as_ref().unwrap().identifier();
        let id = res.as_ref().unwrap().metadata().table_uuid;
        let commit = TableCommit {
            ident: ident.clone(),
            requirements: vec![TableRequirement::UuidMatch { uuid: id }],
            updates: vec![update],
        };

        let res = repo.update_table_ext(commit).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let res_load = repo.load_table(&ident).await;
        assert!(res_load.is_ok(), "{}", res_load.unwrap_err().to_string());

        assert_eq!(
            res_load
                .unwrap()
                .metadata()
                .sort_orders
                .get(&1)
                .unwrap()
                .fields
                .len(),
            2
        );
    }
}
