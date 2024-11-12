use crate::error::{Error, Result};
// TODO: Replace this with this crate error and result
use crate::models::{
    Config, Database, DatabaseIdent, Table, TableCommit, TableIdent, TableRequirementExt,
    WarehouseIdent,
};
use crate::repository::{DatabaseRepository, TableRepository};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use control_plane::models::{StorageProfile, Warehouse};
use control_plane::service::ControlService;
use iceberg::{spec::TableMetadataBuilder, TableCreation};
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use iceberg::spec::FormatVersion;
use uuid::Uuid;

// FIXME: Rename namespace to database: namespace concept is Iceberg REST API specific
// Internally we have not a namespace but a database
// Database is a superset of namespace, Database > Namespace
// We can create namespace from a database, but not otherwise
#[async_trait]
pub trait Catalog: Debug + Sync + Send {
    async fn get_config(&self, ident: Option<WarehouseIdent>, storage_profile: Option<StorageProfile>) ->
                                                                                                       Result<Config>;
    async fn list_namespaces(
        &self,
        warehouse: &WarehouseIdent,
        parent: Option<&DatabaseIdent>,
    ) -> Result<Vec<Database>>;
    async fn create_namespace(
        &self,
        namespace: &DatabaseIdent,
        properties: HashMap<String, String>,
    ) -> Result<Database>;
    async fn get_namespace(&self, namespace: &DatabaseIdent) -> Result<Database>;
    async fn update_namespace(
        &self,
        namespace: &DatabaseIdent,
        properties: HashMap<String, String>,
    ) -> Result<()>;
    async fn drop_namespace(&self, namespace: &DatabaseIdent) -> Result<()>;
    async fn list_tables(&self, namespace: &DatabaseIdent) -> Result<Vec<Table>>;
    // TODO: We need warehouse and storage profile objects here
    // to generate location and actually write metadata contents
    async fn create_table(
        &self,
        namespace: &DatabaseIdent,
        storage_profile: &StorageProfile,
        warehouse: &Warehouse,
        creation: TableCreation,
        properties: Option<HashMap<String, String>>,
    ) -> Result<Table>;
    async fn load_table(&self, table: &TableIdent) -> Result<Table>;
    async fn drop_table(&self, table: &TableIdent) -> Result<()>;
    async fn update_table(&self, storage_profile: &StorageProfile, warehouse: &Warehouse, commit: TableCommit) -> Result<Table>;
}

#[derive(Clone)]
pub struct CatalogImpl {
    table_repo: Arc<dyn TableRepository>,
    db_repo: Arc<dyn DatabaseRepository>,
}

impl Debug for CatalogImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogImpl").finish()
    }
}

impl CatalogImpl {
    pub fn new(table_repo: Arc<dyn TableRepository>, db_repo: Arc<dyn DatabaseRepository>) -> Self {
        Self {
            table_repo,
            db_repo,
        }
    }

    fn generate_metadata_filename() -> String {
        format!("{}.metadata.json", Uuid::new_v4().to_string())
    }
}


#[async_trait]
impl Catalog for CatalogImpl {
    async fn get_config(&self, ident: Option<WarehouseIdent>, storage_profile: Option<StorageProfile>) ->
                                                                                                      Result<Config> {
        // TODO: Implement warehouse config
        // TODO: Should it include prefix from Warehouse or not?
        // As per https://github.com/apache/iceberg-python/blob/main/pyiceberg/catalog/rest.py#L298
        // prefix is used only in URL construction, so no - only id as it's part of the URL
        // TODO: Should it include bucket from storage profile?
        // hardcoding for now
        // uri and prefix
        let mut config = Config {
            defaults: HashMap::new(),
            overrides: HashMap::from([
                // ("warehouse".to_string(), ident.id().to_string()),
                (
                    "uri".to_string(),
                    "http://localhost:3000/catalog".to_string(),
                ),
            ]),
        };
        if let Some(wh_ident) = ident {
            // we parse it as warehouse id in catalog url
            config.overrides.insert("prefix".to_string(), format! {"{}", wh_ident.id()});
        }
        if let Some(sp) = storage_profile {
            config.overrides.insert("s3.endpoint".to_string(), sp.endpoint.clone().unwrap());
        }
        Ok(config)
    }

    async fn update_table(&self,
                          storage_profile: &StorageProfile,
                          warehouse: &Warehouse,
                          commit: TableCommit) -> Result<Table> {
        let table = self.load_table(&commit.ident).await?;

        commit
            .requirements
            .into_iter()
            .map(TableRequirementExt::new)
            .try_for_each(|req| req.assert(&table.metadata, true))?;

        // TODO rewrite metadata file? need to research when metadata rewrite is needed
        // Currently the metadata file is only written once - during table creation

        let mut builder =
            TableMetadataBuilder::new_from_metadata(table.metadata, Some(table.metadata_location.clone()));

        for update in commit.updates {
            builder = update.apply(builder)?;
        }
        let result = builder.build()?;

        let base_part = storage_profile.get_base_url();
        let table_part = format!("{}/{}", warehouse.location, commit.ident.table);
        let metadata_part = format!("metadata/{}", CatalogImpl::generate_metadata_filename());

        let mut properties = table.properties.clone();
        properties.insert("updated_at".to_string(), Utc::now().to_rfc3339());

        let table: Table = Table {
            metadata: result.metadata,
            metadata_location: format!("{base_part}/{table_part}/{metadata_part}"),
            ident: table.ident,
            properties,
        };
        self.table_repo.put(&table).await?;

        let object_store: Box<dyn ObjectStore> = storage_profile.get_object_store();
        let data = Bytes::from(serde_json::to_vec(&table.metadata).unwrap());
        let path = Path::from(format!("{table_part}/{metadata_part}"));
        object_store.put(&path, PutPayload::from(data)).await.unwrap();

        Ok(table)
    }

    /// List namespaces inside the catalog.
    async fn list_namespaces(
        &self,
        warehouse: &WarehouseIdent,
        _parent: Option<&DatabaseIdent>,
    ) -> Result<Vec<Database>> {
        // TODO: Implement parent filtering
        let keys = self.db_repo.list(warehouse).await?;

        Ok(keys)
    }

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        namespace: &DatabaseIdent,
        properties: HashMap<String, String>,
    ) -> Result<Database> {
        let db = Database {
            ident: namespace.clone(),
            properties,
        };
        let database = self.get_namespace(namespace).await;
        if database.is_ok() {
            return Err(Error::ErrAlreadyExists);
        }
        // put bluntly saves to db no matter what
        self.db_repo.put(&db).await?;

        Ok(db)
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, namespace: &DatabaseIdent) -> Result<Database> {
        let db = self.db_repo.get(namespace).await?;

        Ok(db)
    }
    /// Update a namespace inside the catalog.
    ///
    /// # Behavior
    ///
    /// The properties must be the full set of namespace.
    async fn update_namespace(
        &self,
        namespace: &DatabaseIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        // Check if the namespace exists
        _ = self.get_namespace(namespace).await?;
        let params = Database {
            ident: namespace.clone(),
            properties,
        };
        self.db_repo.put(&params).await?;
        Ok(())
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace: &DatabaseIdent) -> Result<()> {
        // Check if the namespace exists
        _ = self.get_namespace(namespace).await?;
        // Check if there are tables in the namespace
        let tables = self.list_tables(namespace).await?;
        if !tables.is_empty() {
            return Err(Error::ErrNotEmpty);
        }

        self.db_repo.delete(namespace).await?;

        Ok(())
    }

    /// List tables from namespace.
    async fn list_tables(&self, namespace: &DatabaseIdent) -> Result<Vec<Table>> {
        // Check namespace exists
        _ = self.get_namespace(namespace).await?;

        let tables = self.table_repo.list(namespace).await?;

        Ok(tables)
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace: &DatabaseIdent,
        storage_profile: &StorageProfile,
        warehouse: &Warehouse,
        table_creation: TableCreation,
        properties: Option<HashMap<String, String>>,
    ) -> Result<Table> {
        // Check if namespace exists
        _ = self.get_namespace(namespace).await?;
        // Check if table exists
        let ident = TableIdent {
            database: namespace.clone(),
            table: table_creation.name.clone(),
        };
        let res = self.load_table(&ident).await;
        if res.is_ok() {
            return Err(Error::ErrAlreadyExists);
        }

        // TODO: Robust location generation
        // Take into account namespace location property if present
        // Take into account provided location if present
        // If none, generate location based on warehouse location

        let base_part = storage_profile.get_base_url();
        let table_part = format!("{}/{}", warehouse.location, table_creation.name);
        let metadata_part = format!("metadata/{}", CatalogImpl::generate_metadata_filename());

        let table_creation = {
            let mut creation = table_creation;
            creation.location = Some(format!("{base_part}/{table_part}"));
            creation
        };

        // TODO: Add checks
        // - Check if storage profile is valid (writable)

        let table_name = table_creation.name.clone();
        let result = TableMetadataBuilder::from_table_creation(table_creation)?
            .upgrade_format_version(FormatVersion::V2)?
            .build()?;
        let metadata = result.metadata.clone();

        let table = Table {
            metadata: metadata.clone(),
            metadata_location: format!("{base_part}/{table_part}/{metadata_part}"),
            ident: TableIdent {
                database: namespace.clone(),
                table: table_name.clone(),
            },
            properties: properties.unwrap_or_default(),
        };
        self.table_repo.put(&table).await?;

        let object_store: Box<dyn ObjectStore> = storage_profile.get_object_store();
        let data = Bytes::from(serde_json::to_vec(&table.metadata).unwrap());
        let path = Path::from(format!("{table_part}/{metadata_part}"));
        object_store.put(&path, PutPayload::from(data)).await.unwrap();

        Ok(table)
    }

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        let table = self.table_repo.get(table).await?;
        Ok(table)
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        // Check if table exists
        _ = self.load_table(table).await?;
        self.table_repo.delete(table).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::{DatabaseRepositoryDb, TableRepositoryDb};
    use iceberg::NamespaceIdent;
    use object_store_for_slatedb::{memory::InMemory, path::Path, ObjectStore};
    use slatedb::config::DbOptions;
    use slatedb::db::Db as SlateDb;
    use std::sync::Arc;
    use utils::Db;
    use uuid::Uuid;

    async fn create_service() -> impl Catalog {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = DbOptions::default();
        let db = Arc::new(Db::new(
            SlateDb::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
                .await
                .unwrap(),
        ));

        let t_repo = TableRepositoryDb::new(db.clone());
        let db_repo = DatabaseRepositoryDb::new(db.clone());

        CatalogImpl::new(Arc::new(t_repo), Arc::new(db_repo))
    }

    fn test_database() -> Database {
        Database {
            ident: DatabaseIdent {
                warehouse: WarehouseIdent::new(uuid::Uuid::new_v4()),
                namespace: NamespaceIdent::new("ns_test".to_string()),
            },
            properties: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_create_namespace_success() {
        let service = create_service().await;
        let ident = test_database().ident;
        let properties = HashMap::new();

        let res = service.create_namespace(&ident, properties).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let res = service.get_namespace(&ident).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());
        assert_eq!(res.unwrap().ident, ident);
    }

    #[tokio::test]
    async fn test_create_namespace_already_exists() {
        let service = create_service().await;
        let ident = test_database().ident;

        let res = service.create_namespace(&ident, HashMap::default()).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let res = service.create_namespace(&ident, HashMap::default()).await;
        assert!(res.is_err(), "{}", res.unwrap_err().to_string());
    }

    #[tokio::test]
    async fn test_get_namespace_not_found() {
        let service = create_service().await;
        let ident = test_database().ident;

        let res = service.get_namespace(&ident).await;
        assert!(res.is_err(), "{}", res.unwrap_err().to_string());
    }

    // 8. Test updating an existing namespace's properties
    #[tokio::test]
    async fn test_update_namespace_success() {
        let service = create_service().await;
        let ident = test_database().ident;
        let properties = HashMap::new();

        let res = service.create_namespace(&ident, properties).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let properties = [("key".to_string(), "value".to_string())]
            .iter()
            .cloned()
            .collect();

        let res = service.update_namespace(&ident, properties).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let res = service.get_namespace(&ident).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());
        assert_eq!(res.unwrap().properties.len(), 1);
    }

    // 9. Test updating a non-existent namespace
    #[tokio::test]
    async fn test_update_namespace_not_found() {
        let service = create_service().await;
        let ident = test_database().ident;
        let properties = HashMap::new();

        let res = service.update_namespace(&ident, properties).await;
        assert!(res.is_err(), "{}", res.unwrap_err().to_string());
    }

    // 10. Test dropping an existing namespace
    #[tokio::test]
    async fn test_drop_namespace_success() {
        let service = create_service().await;
        let ident = test_database().ident;
        let properties = HashMap::new();

        let res = service.create_namespace(&ident, properties).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let res = service.drop_namespace(&ident).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let res = service.get_namespace(&ident).await;
        assert!(res.is_err(), "{}", res.unwrap_err().to_string());
    }

    // 11. Test dropping a non-existent namespace
    #[tokio::test]
    async fn test_drop_namespace_not_found() {
        let service = create_service().await;
        let ident = test_database().ident;

        let res = service.drop_namespace(&ident).await;
        assert!(res.is_err(), "{}", res.unwrap_err().to_string());
    }

    // 12. Test listing all namespaces in a warehouse
    #[tokio::test]
    async fn test_list_namespaces_returns_all() {
        let service = create_service().await;
        let wh_ident = WarehouseIdent::new(Uuid::new_v4());
        for t in 0..2 {
            let ident = DatabaseIdent {
                warehouse: wh_ident.clone(),
                namespace: NamespaceIdent::new(format!("ns_{t}")),
            };
            let properties = HashMap::new();
            let res = service.create_namespace(&ident, properties).await;
            assert!(res.is_ok(), "{}", res.unwrap_err().to_string());
        }

        let res = service.list_namespaces(&wh_ident, None).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());
        assert_eq!(res.unwrap().len(), 2);
    }

    // 13. Test listing namespaces with a parent filter (if implemented)
    #[tokio::test]
    #[should_panic = "Not implemented"]
    async fn test_list_namespaces_with_parent_filter() {
        // TODO: implement test
        panic!("Not implemented");
    }

    // 14. Test creating a new table successfully in an existing namespace
    #[tokio::test]
    async fn test_create_table_success() {
        let service = create_service().await;
        let ident = test_database().ident;
        let properties = HashMap::new();

        let res = service.create_namespace(&ident, properties).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let data = r#"{
            "type":"struct",
            "schema-id":1,
            "fields":[
                {
                    "id":1,
                    "name":"foo",
                    "required":false,
                    "type":"string"
                },
                {
                    "id":2,
                    "name":"bar",
                    "required":true,
                    "type":"int"
                },
                {
                    "id":3,
                    "name":"baz",
                    "required":false,
                    "type":"boolean"
                }
            ],
            "identifier-field-ids":[2]
        }"#;
        let schema = serde_json::from_str(data).expect("Failed to parse schema");
        let creation = TableCreation {
            name: "test_table".to_string(),
            schema,
            location: Some("s3://bucket/path".to_string()),
            partition_spec: None,
            properties: HashMap::new(),
            sort_order: None,
        };
        let res = service.create_table(&ident, creation).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());
        let tident = res.unwrap().ident;

        let schema = serde_json::from_str(data).expect("Failed to parse schema");
        let creation = TableCreation {
            name: "test_table".to_string(),
            schema,
            location: Some("s3://bucket/path".to_string()),
            partition_spec: None,
            properties: HashMap::new(),
            sort_order: None,
        };
        let res = service.create_table(&ident, creation).await;
        assert!(res.is_err());

        let res = service.load_table(&tident).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let res = service.list_tables(&ident).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());
        assert_eq!(res.unwrap().len(), 1);
    }

    // 15. Test creating a table in a non-existent namespace
    #[tokio::test]
    async fn test_create_table_namespace_not_found() {
        let service = create_service().await;
        let ident = test_database().ident;

        let data = r#"{
            "type":"struct",
            "schema-id":1,
            "fields":[
                {
                    "id":1,
                    "name":"foo",
                    "required":false,
                    "type":"string"
                },
                {
                    "id":2,
                    "name":"bar",
                    "required":true,
                    "type":"int"
                },
                {
                    "id":3,
                    "name":"baz",
                    "required":false,
                    "type":"boolean"
                }
            ],
            "identifier-field-ids":[2]
        }"#;
        let schema = serde_json::from_str(data).expect("Failed to parse schema");
        let creation = TableCreation {
            name: "test_table".to_string(),
            schema,
            location: Some("s3://bucket/path".to_string()),
            partition_spec: None,
            properties: HashMap::new(),
            sort_order: None,
        };

        let res = service.create_table(&ident, creation).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_load_table_not_found() {
        let service = create_service().await;
        let ident = TableIdent {
            database: test_database().ident,
            table: "test_table".to_string(),
        };

        let res = service.load_table(&ident).await;
        assert!(res.is_err());
    }

    // 21. Test dropping a non-existent table
    #[tokio::test]
    async fn test_drop_table_not_found() {
        let service = create_service().await;
        let ident = TableIdent {
            database: test_database().ident,
            table: "test_table".to_string(),
        };
        let res = service.drop_table(&ident).await;
        assert!(res.is_err());
    }

    // 23. Test listing tables in a non-existent namespace
    #[tokio::test]
    async fn test_list_tables_namespace_not_found() {
        let service = create_service().await;
        let ident = test_database().ident;

        let res = service.list_tables(&ident).await;
        assert!(res.is_err());
    }

    // 27. Test updating a non-existent table
    #[tokio::test]
    async fn test_update_table_not_found() {
        let service = create_service().await;
        let ident = TableIdent {
            database: test_database().ident,
            table: "test_table".to_string(),
        };
        let commit = TableCommit {
            ident,
            requirements: vec![],
            updates: vec![],
        };

        let res = service.update_table(commit).await;
        assert!(res.is_err());
    }

    // 29. Test creating a table with invalid schema or properties
    #[tokio::test]
    async fn test_create_table_invalid_schema() {
        let service = create_service().await;
        let ident = test_database().ident;
        let properties = HashMap::new();

        let res = service.create_namespace(&ident, properties).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let data = r#"{
            "type":"struct",
            "schema-id":1,
            "fields":[
                {
                    "id":1,
                    "name":"foo",
                    "required":false,
                    "type":"string"
                },
                {
                    "id":2,
                    "name":"bar",
                    "required":true,
                    "type":"int"
                },
                {
                    "id":3,
                    "name":"baz",
                    "required":false,
                    "type":"boolean"
                }
            ],
            "identifier-field-ids":[2]
        }"#;
        let schema = serde_json::from_str(data).expect("Failed to parse schema");
        let creation = TableCreation {
            name: "test_table".to_string(),
            schema,
            location: None,
            partition_spec: None,
            properties: HashMap::new(),
            sort_order: None,
        };

        let res = service.create_table(&ident, creation).await;
        assert!(res.is_err());
    }

    // 30. Test dropping a namespace that still contains tables
    #[tokio::test]
    async fn test_drop_namespace_with_existing_tables() {
        let service = create_service().await;
        let ident = test_database().ident;
        let properties = HashMap::new();

        let res = service.create_namespace(&ident, properties).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let data = r#"{
            "type":"struct",
            "schema-id":1,
            "fields":[
                {
                    "id":1,
                    "name":"foo",
                    "required":false,
                    "type":"string"
                },
                {
                    "id":2,
                    "name":"bar",
                    "required":true,
                    "type":"int"
                },
                {
                    "id":3,
                    "name":"baz",
                    "required":false,
                    "type":"boolean"
                }
            ],
            "identifier-field-ids":[2]
        }"#;
        let schema = serde_json::from_str(data).expect("Failed to parse schema");
        let creation = TableCreation {
            name: "test_table".to_string(),
            schema,
            location: Some("s3://bucket/path".to_string()),
            partition_spec: None,
            properties: HashMap::new(),
            sort_order: None,
        };
        let res = service.create_table(&ident, creation).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let res = service.drop_namespace(&ident).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_update_table() {
        let service = create_service().await;
        let ident = test_database().ident;
        let properties = HashMap::new();

        let res = service.create_namespace(&ident, properties).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let data = r#"{
            "type":"struct",
            "schema-id":1,
            "fields":[
                {
                    "id":1,
                    "name":"foo",
                    "required":false,
                    "type":"string"
                },
                {
                    "id":2,
                    "name":"bar",
                    "required":true,
                    "type":"int"
                },
                {
                    "id":3,
                    "name":"baz",
                    "required":false,
                    "type":"boolean"
                }
            ],
            "identifier-field-ids":[2]
        }"#;
        let schema = serde_json::from_str(data).expect("Failed to parse schema");
        let creation = TableCreation {
            name: "test_table".to_string(),
            schema,
            location: Some("s3://bucket/path".to_string()),
            partition_spec: None,
            properties: HashMap::new(),
            sort_order: None,
        };
        let res = service.create_table(&ident, creation).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let json = r#"
{
    "action": "add-sort-order",
    "sort-order": {
        "order-id": 1,
        "fields": [
            {
                "transform": "identity",
                "source-id": 2,
                "direction": "asc",
                "null-order": "nulls-first"
            }
        ]
    }
}
        "#;
        let update: iceberg::TableUpdate = serde_json::from_str(json).unwrap();

        let commit = TableCommit {
            ident: res.unwrap().ident,
            requirements: vec![],
            updates: vec![update],
            properties: None,
        };

        let res = service.update_table(commit).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());

        let res = service.load_table(&res.unwrap().ident).await;
        assert!(res.is_ok(), "{}", res.unwrap_err().to_string());
        assert_eq!(
            res.expect("Failed to get table")
                .metadata
                .sort_orders
                .get(&1)
                .unwrap()
                .fields
                .len(),
            1
        );
    }
}
