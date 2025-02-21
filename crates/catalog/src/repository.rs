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

use async_trait::async_trait;
use std::sync::Arc;

use utils::Db;

use crate::error::CatalogResult;
use crate::models::{Database, DatabaseIdent, Table, TableIdent, WarehouseIdent};

const DBPREFIX: &str = "db";
//const SEP: &str = "\u{001f}";
const TBLPREFIX: &str = "tbl";
//const ALL: &str = "all";

#[async_trait]
pub trait TableRepository: Send + Sync {
    async fn put(&self, params: &Table) -> CatalogResult<()>;
    async fn get(&self, id: &TableIdent) -> CatalogResult<Table>;
    async fn delete(&self, id: &TableIdent) -> CatalogResult<()>;
    async fn list(&self, db: &DatabaseIdent) -> CatalogResult<Vec<Table>>;
}

#[async_trait]
pub trait DatabaseRepository: Send + Sync {
    async fn put(&self, params: &Database) -> CatalogResult<()>;
    async fn get(&self, id: &DatabaseIdent) -> CatalogResult<Database>;
    async fn delete(&self, id: &DatabaseIdent) -> CatalogResult<()>;
    async fn list(&self, wh: &WarehouseIdent) -> CatalogResult<Vec<Database>>;
}

pub struct TableRepositoryDb {
    db: Arc<Db>,
}

impl TableRepositoryDb {
    pub const fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl TableRepository for TableRepositoryDb {
    #[tracing::instrument(level = "trace", err, skip(self))]
    async fn put(&self, params: &Table) -> CatalogResult<()> {
        let key = format!("{TBLPREFIX}.{}", params.ident);
        self.db.put(&key, &params).await?;
        self.db
            .append(&format!("{TBLPREFIX}.{}", params.ident.database), key)
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", err, skip(self))]
    async fn get(&self, id: &TableIdent) -> CatalogResult<Table> {
        let key = format!("{TBLPREFIX}.{id}");
        let table = self.db.get(&key).await?;
        let table = table.ok_or(crate::error::CatalogError::TableNotFound { key })?;
        Ok(table)
    }

    #[tracing::instrument(level = "trace", err, skip(self))]
    async fn delete(&self, id: &TableIdent) -> CatalogResult<()> {
        let key = format!("{TBLPREFIX}.{id}");
        self.db.delete(&key).await?;
        self.db
            .remove(&format!("{TBLPREFIX}.{}", id.database), &key)
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", err, skip(self))]
    async fn list(&self, db: &DatabaseIdent) -> CatalogResult<Vec<Table>> {
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
    #[tracing::instrument(level = "trace", err, skip(self))]
    async fn put(&self, params: &Database) -> CatalogResult<()> {
        let key = format!("{DBPREFIX}.{}", params.ident);
        self.db.put(&key, &params).await?;
        self.db
            .append(&format!("{DBPREFIX}.{}", params.ident.warehouse), key)
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", err, skip(self))]
    async fn get(&self, id: &DatabaseIdent) -> CatalogResult<Database> {
        let key = format!("{DBPREFIX}.{id}");
        let db = self.db.get(&key).await?;
        let db = db.ok_or(crate::error::CatalogError::DatabaseNotFound { key })?;
        Ok(db)
    }

    #[tracing::instrument(level = "trace", err, skip(self))]
    async fn delete(&self, id: &DatabaseIdent) -> CatalogResult<()> {
        let key = format!("{DBPREFIX}.{id}");
        self.db.delete(&key).await?;
        self.db
            .remove(&format!("{DBPREFIX}.{}", id.warehouse), &key)
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", err, skip(self))]
    async fn list(&self, wh: &WarehouseIdent) -> CatalogResult<Vec<Database>> {
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
    pub const fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use iceberg::spec::TableMetadata;
    use iceberg::NamespaceIdent;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use slatedb::config::DbOptions;
    use slatedb::db::Db as SlateDb;
    use std::collections::HashMap;
    use std::sync::Arc;
    use utils::Db;
    use uuid::Uuid;

    async fn create_db() -> Db {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = DbOptions::default();
        Db::new(
            SlateDb::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
                .await
                .unwrap(),
        )
    }

    fn create_database() -> Database {
        Database {
            ident: DatabaseIdent {
                warehouse: WarehouseIdent::new(Uuid::new_v4()),
                namespace: NamespaceIdent::new("dbname".to_string()),
            },
            properties: HashMap::default(),
        }
    }

    fn create_table() -> Table {
        Table {
            ident: TableIdent {
                database: DatabaseIdent {
                    warehouse: WarehouseIdent::new(Uuid::new_v4()),
                    namespace: NamespaceIdent::new("dbname".to_string()),
                },
                table: "tblname".to_string(),
            },
            metadata_location: "s3://bucket/path".to_string(),
            metadata: create_table_metadata(),
            properties: HashMap::default(),
        }
    }

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
    async fn test_put_table() {
        let db = create_db().await;
        let repo = TableRepositoryDb::new(Arc::new(db));
        let table = create_table();
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
    }

    #[tokio::test]
    async fn test_get_table() {
        let db = create_db().await;
        let repo = TableRepositoryDb::new(Arc::new(db));
        let table = create_table();

        repo.put(&table).await.expect("failed to create table");
        let fetched_table = repo.get(&table.ident).await.expect("failed to get table");
        assert_eq!(table.ident, fetched_table.ident);
    }

    #[tokio::test]
    async fn test_get_non_existent_table() {
        let db = create_db().await;
        let repo = TableRepositoryDb::new(Arc::new(db));
        let table_ident = TableIdent {
            database: DatabaseIdent {
                warehouse: WarehouseIdent::new(Uuid::new_v4()),
                namespace: NamespaceIdent::new("non_existent_db".to_string()),
            },
            table: "non_existent_table".to_string(),
        };

        let result = repo.get(&table_ident).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_table() {
        let db = create_db().await;
        let repo = TableRepositoryDb::new(Arc::new(db));
        let table = create_table();

        repo.put(&table).await.expect("failed to create table");
        repo.delete(&table.ident)
            .await
            .expect("failed to delete table");

        let list = repo
            .list(&table.ident.database)
            .await
            .expect("failed to list tables");
        assert_eq!(list.len(), 0);
    }

    #[tokio::test]
    async fn test_delete_non_existent_table() {
        let db = create_db().await;
        let repo = TableRepositoryDb::new(Arc::new(db));
        let table_ident = TableIdent {
            database: DatabaseIdent {
                warehouse: WarehouseIdent::new(Uuid::new_v4()),
                namespace: NamespaceIdent::new("non_existent_db".to_string()),
            },
            table: "non_existent_table".to_string(),
        };

        let result = repo.delete(&table_ident).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_list_table() {
        let db = create_db().await;
        let repo = TableRepositoryDb::new(Arc::new(db));
        let table = create_table();

        repo.put(&table).await.expect("failed to create table");
        let list = repo
            .list(&table.ident.database)
            .await
            .expect("failed to list tables");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].ident, table.ident);
    }

    #[tokio::test]
    async fn test_list_table_empty() {
        let db = create_db().await;
        let repo = TableRepositoryDb::new(Arc::new(db));
        let database_ident = DatabaseIdent {
            warehouse: WarehouseIdent::new(Uuid::new_v4()),
            namespace: NamespaceIdent::new("empty_db".to_string()),
        };

        let list = repo
            .list(&database_ident)
            .await
            .expect("failed to list tables");
        assert_eq!(list.len(), 0);
    }

    #[tokio::test]
    async fn test_put_database() {
        let db = create_db().await;
        let repo = DatabaseRepositoryDb::new(Arc::new(db));
        let database = create_database();
        repo.put(&database)
            .await
            .expect("failed to create database");

        let list = repo
            .list(&database.ident.warehouse)
            .await
            .expect("failed to list databases");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].ident, database.ident);

        // Check no extra databases
        let list = repo
            .list(&WarehouseIdent::new(Uuid::default()))
            .await
            .expect("failed to list databases");
        assert_eq!(list.len(), 0);
    }

    #[tokio::test]
    async fn test_get_database() {
        let db = create_db().await;
        let repo = DatabaseRepositoryDb::new(Arc::new(db));
        let database = create_database();

        repo.put(&database)
            .await
            .expect("failed to create database");
        let fetched_database = repo
            .get(&database.ident)
            .await
            .expect("failed to get database");
        assert_eq!(database.ident, fetched_database.ident);
    }

    #[tokio::test]
    async fn test_delete_database() {
        let db = create_db().await;
        let repo = DatabaseRepositoryDb::new(Arc::new(db));
        let database = create_database();

        repo.put(&database)
            .await
            .expect("failed to create database");
        repo.delete(&database.ident)
            .await
            .expect("failed to delete database");

        let list = repo
            .list(&database.ident.warehouse)
            .await
            .expect("failed to list databases");
        assert_eq!(list.len(), 0);
    }

    #[tokio::test]
    async fn test_list_database() {
        let db = create_db().await;
        let repo = DatabaseRepositoryDb::new(Arc::new(db));
        let database = create_database();

        repo.put(&database)
            .await
            .expect("failed to create database");
        let list = repo
            .list(&database.ident.warehouse)
            .await
            .expect("failed to list databases");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].ident, database.ident);
    }

    #[tokio::test]
    async fn test_list_empty_warehouse() {
        let db = create_db().await;
        let repo = DatabaseRepositoryDb::new(Arc::new(db));
        let warehouse_ident = WarehouseIdent::new(Uuid::new_v4());

        let list = repo
            .list(&warehouse_ident)
            .await
            .expect("failed to list databases");
        assert_eq!(list.len(), 0);
    }

    #[tokio::test]
    async fn test_get_non_existent_database() {
        let db = create_db().await;
        let repo = DatabaseRepositoryDb::new(Arc::new(db));
        let database_ident = DatabaseIdent {
            warehouse: WarehouseIdent::new(Uuid::new_v4()),
            namespace: NamespaceIdent::new("non_existent_db".to_string()),
        };

        let result = repo.get(&database_ident).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_non_existent_database() {
        let db = create_db().await;
        let repo = DatabaseRepositoryDb::new(Arc::new(db));
        let database_ident = DatabaseIdent {
            warehouse: WarehouseIdent::new(Uuid::new_v4()),
            namespace: NamespaceIdent::new("non_existent_db".to_string()),
        };

        let result = repo.delete(&database_ident).await;
        assert!(result.is_ok());
    }
}
