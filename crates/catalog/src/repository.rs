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

const SEP: u8 = b',';
const DBLIST: &[u8] = b"db.all";
const _DBPREFIX: &[u8] = b"db";
const _TBLLIST: &[u8] = b"tbl.all";
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
}

#[async_trait]
impl Catalog for DbRepository {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let list =
            self.db.get(DBLIST).await.map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to read db").with_source(e)
            })?;
        let list = list.unwrap_or_default();

        let list: std::result::Result<Vec<_>, _> = list
            .split(|&c| c == SEP)
            .map(|b| std::str::from_utf8(b))
            .collect();
        let list = list.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to parse db list").with_source(e)
        })?;

        Ok(list
            .into_iter()
            .map(|s| NamespaceIdent::new(s.to_string()))
            .collect())
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "create namespace is not supported",
        ))
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> Result<Namespace> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "get namespace is not supported",
        ))
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

    async fn drop_namespace(&self, _amespace: &NamespaceIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "drop namespace is not supported",
        ))
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let warehouse = self.warehouse.id;
        let database = namespace.to_url_string();
        let key = format!("{TBLPREFIX}.{warehouse}.{database}.{ALL}");
        let list = self.db.get(key.as_bytes()).await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to read table").with_source(e)
        })?;

        let all_keys: Vec<String> = list.filter(|bytes| !bytes.is_empty()).map_or_else(
            || Ok(Vec::new()),
            |bytes| {
                de::from_slice(&bytes).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "failed to deserialize keys").with_source(e)
                })
            },
        )?;

        Ok(all_keys
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
        let value = ser::to_vec(&metadata).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to serialize metadata").with_source(e)
        })?;
        self.db.put(key.as_bytes(), value.as_ref()).await;

        // Update special key all value
        let key = format!("{TBLPREFIX}.{warehouse}.{database}.{ALL}");
        let value: Option<bytes::Bytes> = self.db.get(key.as_bytes()).await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to read table").with_source(e)
        })?;

        let mut all_keys = value.filter(|bytes| !bytes.is_empty()).map_or_else(
            || Ok(Vec::new()),
            |bytes| {
                de::from_slice(&bytes).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "failed to deserialize keys").with_source(e)
                })
            },
        )?;
        all_keys.push(name.clone());

        let value = ser::to_vec(&all_keys).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to serialize keys").with_source(e)
        })?;
        self.db.put(key.as_bytes(), value.as_ref()).await;

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
        let value = self.db.get(key.as_bytes()).await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to read table").with_source(e)
        })?;
        let value = value.ok_or_else(|| Error::new(ErrorKind::DataInvalid, "table not found"))?;
        let metadata: TableMetadata = serde_json::from_slice(&value).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to deserialize metadata").with_source(e)
        })?;
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
        let value: Option<bytes::Bytes> = self.db.get(key.as_bytes()).await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to read table").with_source(e)
        })?;

        let mut all_keys: Vec<String> = value.filter(|bytes| !bytes.is_empty()).map_or_else(
            || Ok(Vec::new()),
            |bytes| {
                de::from_slice(&bytes).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "failed to deserialize keys").with_source(e)
                })
            },
        )?;
        all_keys.retain(|key| *key != table.name);

        let value = ser::to_vec(&all_keys).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to serialize keys").with_source(e)
        })?;
        self.db.put(key.as_bytes(), value.as_ref()).await;

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
    use bytes::Bytes;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use slatedb::config::DbOptions;
    use slatedb::db::Db;
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_empty_bytes() {
        let cases = [Bytes::new(), Bytes::from_static(b"")];

        for input in cases {
            let list: std::result::Result<Vec<_>, _> = input
                .split(|&c| c == SEP)
                .filter(|b| !b.is_empty())
                .map(|b| std::str::from_utf8(b))
                .collect();
            assert_eq!(list, Ok(vec![]));
            assert_eq!(list.unwrap().len(), 0);
        }
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
