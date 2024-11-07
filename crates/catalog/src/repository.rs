use async_trait::async_trait;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use uuid::Uuid;

use utils::Db;
use utils::{Entity, Repository};

use crate::error::Result;
use crate::models::{Database, DatabaseIdent, Table, TableIdent, WarehouseIdent};

const DBPREFIX: &str = "db";
const SEP: &str = "\u{001f}";
const TBLPREFIX: &str = "tbl";
const ALL: &str = "all";

#[async_trait]
pub trait TableRepository: Send + Sync {
    async fn put(&self, params: &Table) -> Result<()>;
    async fn get(&self, id: &TableIdent) -> Result<Table>;
    async fn delete(&self, id: &TableIdent) -> Result<()>;
    async fn list(&self, db: &DatabaseIdent) -> Result<Vec<Table>>;
}

#[async_trait]
pub trait DatabaseRepository: Send + Sync {
    async fn put(&self, params: &Database) -> Result<()>;
    async fn get(&self, id: &DatabaseIdent) -> Result<Database>;
    async fn delete(&self, id: &DatabaseIdent) -> Result<()>;
    async fn list(&self, wh: &WarehouseIdent) -> Result<Vec<Database>>;
}

pub struct TableRepositoryDb {
    db: Arc<Db>,
}

impl TableRepositoryDb {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

impl Display for TableIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.database, self.table)
    }
}

impl Display for DatabaseIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.warehouse, self.namespace.to_url_string())
    }
}

impl Display for WarehouseIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id())
    }
}

#[async_trait]
impl TableRepository for TableRepositoryDb {
    async fn put(&self, params: &Table) -> Result<()> {
        let key = format!("{TBLPREFIX}.{}", params.ident);
        self.db.put(&key, &params).await?;
        self.db
            .append(&format!("{TBLPREFIX}.{}", params.ident.database), key)
            .await?;
        Ok(())
    }

    async fn get(&self, id: &TableIdent) -> Result<Table> {
        let key = format!("{TBLPREFIX}.{id}");
        let table = self.db.get(&key).await?;
        let table = table.ok_or(crate::error::Error::ErrNotFound)?;
        Ok(table)
    }

    async fn delete(&self, id: &TableIdent) -> Result<()> {
        let key = format!("{TBLPREFIX}.{id}");
        self.db.delete(&key).await?;
        self.db
            .remove(&format!("{TBLPREFIX}.{}", id.database), &key)
            .await?;
        Ok(())
    }

    async fn list(&self, db: &DatabaseIdent) -> Result<Vec<Table>> {
        let key = &format!("{TBLPREFIX}.{db}");
        let keys = self.db.keys(key).await?;
        let futures = keys.iter().map(|key| self.db.get(key)).collect::<Vec<_>>();
        let results = futures::future::try_join_all(futures).await?;
        let entities = results.into_iter().flatten().collect::<Vec<_>>();
        Ok(entities)
    }
}

#[async_trait]
impl DatabaseRepository for DatabaseRepositoryDb {
    async fn put(&self, params: &Database) -> Result<()> {
        let key = format!("{DBPREFIX}.{}", params.ident);
        self.db.put(&key, &params).await?;
        self.db
            .append(&format!("{DBPREFIX}.{}", params.ident.warehouse), key)
            .await?;
        Ok(())
    }

    async fn get(&self, id: &DatabaseIdent) -> Result<Database> {
        let key = format!("{DBPREFIX}.{id}");
        let db = self.db.get(&key).await?;
        let db = db.ok_or(crate::error::Error::ErrNotFound)?;
        Ok(db)
    }

    async fn delete(&self, id: &DatabaseIdent) -> Result<()> {
        let key = format!("{DBPREFIX}.{id}");
        self.db.delete(&key).await?;
        self.db
            .remove(&format!("{DBPREFIX}.{}", id.warehouse), &key)
            .await?;
        Ok(())
    }

    async fn list(&self, wh: &WarehouseIdent) -> Result<Vec<Database>> {
        let key = &format!("{DBPREFIX}.{wh}");
        let keys = self.db.keys(key).await?;
        let futures = keys.iter().map(|key| self.db.get(key)).collect::<Vec<_>>();
        let results = futures::future::try_join_all(futures).await?;
        let entities = results.into_iter().flatten().collect::<Vec<_>>();
        Ok(entities)
    }
}

pub struct DatabaseRepositoryDb {
    db: Arc<Db>,
}

impl DatabaseRepositoryDb {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::TableMetadata;
    use iceberg::NamespaceIdent;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use slatedb::config::DbOptions;
    use slatedb::db::Db as SlateDb;
    use std::sync::Arc;
    use utils::Db;
    use uuid::Uuid;

    fn create_table_metadata() -> TableMetadata {
        let data = r#"
            {
                "format-version" : 2,
                "table-uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
                "location": "s3://b/wh/data.db/table",
                "last-sequence-number" : 1,
                "last-updated-ms": 1515100955770,
                "last-column-id": 1,
                "schemas": [
                    {
                        "schema-id" : 1,
                        "type" : "struct",
                        "fields" :[
                            {
                                "id": 1,
                                "name": "struct_name",
                                "required": true,
                                "type": "fixed[1]"
                            },
                            {
                                "id": 4,
                                "name": "ts",
                                "required": true,
                                "type": "timestamp"
                            }
                        ]
                    }
                ],
                "current-schema-id" : 1,
                "partition-specs": [
                    {
                        "spec-id": 0,
                        "fields": [
                            {
                                "source-id": 4,
                                "field-id": 1000,
                                "name": "ts_day",
                                "transform": "day"
                            }
                        ]
                    }
                ],
                "default-spec-id": 0,
                "last-partition-id": 1000,
                "properties": {
                    "commit.retry.num-retries": "1"
                },
                "metadata-log": [
                    {
                        "metadata-file": "s3://bucket/.../v1.json",
                        "timestamp-ms": 1515100
                    }
                ],
                "sort-orders": [
                    {
                    "order-id": 0,
                    "fields": []
                    }
                ],
                "default-sort-order-id": 0
            }
        "#;

        let metadata: TableMetadata = serde_json::from_str(data).expect("Failed to parse metadata");
        metadata
    }

    #[tokio::test]
    async fn test_table_repo() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = DbOptions::default();
        let db = Db::new(
            SlateDb::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
                .await
                .unwrap(),
        );

        let repo = TableRepositoryDb::new(Arc::new(db));

        let table = Table {
            ident: TableIdent {
                database: DatabaseIdent {
                    warehouse: WarehouseIdent::new(Uuid::new_v4()),
                    namespace: NamespaceIdent::new("dbname".to_string()),
                },
                table: "tblname".to_string(),
            },
            metadata_location: "s3://bucket/path".to_string(),
            metadata: create_table_metadata(),
        };

        repo.put(&table).await.expect("failed to create table");
        let list = repo
            .list(&table.ident.database)
            .await
            .expect("failed to list tables");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].ident, table.ident);

        // Check no extra tables
        let list = repo
            .list(&DatabaseIdent {
                warehouse: WarehouseIdent::new(Uuid::default()),
                namespace: NamespaceIdent::new("dbname".to_string()),
            })
            .await
            .expect("failed to list tables");
        assert_eq!(list.len(), 0);

        let table_2 = repo.get(&table.ident).await.expect("failed to get table");
        assert_eq!(table.ident, table_2.ident);

        repo.delete(&table.ident)
            .await
            .expect("failed to delete table");

        let list = repo
            .list(&table.ident.database)
            .await
            .expect("failed to list tables");
        assert_eq!(list.len(), 0);
    }
}
