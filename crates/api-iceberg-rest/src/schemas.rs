use core_metastore::{
    RwObject, Schema as MetastoreSchema, SchemaIdent as MetastoreSchemaIdent,
    Table as MetastoreTable, TableCreateRequest as MetastoreTableCreateRequest,
    TableFormat as MetastoreTableFormat, TableIdent as MetastoreTableIdent,
    TableUpdate as MetastoreTableUpdate, VolumeIdent as MetastoreVolumeIdent,
};
use iceberg_rest_catalog::models::{
    CreateNamespaceRequest, CreateNamespaceResponse, CreateTableRequest, GetNamespaceResponse,
    ListNamespacesResponse, ListTablesResponse,
};
use iceberg_rust::catalog::commit::{TableRequirement, TableUpdate as IcebergTableUpdate};
use iceberg_rust_spec::identifier::Identifier;
use serde::{Deserialize, Serialize};

#[must_use]
pub fn to_schema(request: CreateNamespaceRequest, db: String) -> MetastoreSchema {
    MetastoreSchema {
        ident: MetastoreSchemaIdent {
            schema: request
                .namespace
                .first()
                .unwrap_or(&String::new()).clone(),
            database: db,
        },
        properties: request.properties,
    }
}

#[must_use]
pub fn from_schema(schema: MetastoreSchema) -> CreateNamespaceResponse {
    CreateNamespaceResponse {
        namespace: vec![schema.ident.database],
        properties: schema.properties,
    }
}

#[must_use]
pub fn from_get_schema(schema: MetastoreSchema) -> GetNamespaceResponse {
    GetNamespaceResponse {
        namespace: vec![schema.ident.database],
        properties: schema.properties,
    }
}

#[must_use]
pub fn to_create_table(
    table: CreateTableRequest,
    table_ident: MetastoreTableIdent,
    volume_ident: Option<MetastoreVolumeIdent>,
) -> MetastoreTableCreateRequest {
    MetastoreTableCreateRequest {
        ident: table_ident,
        properties: table.properties,
        format: Some(MetastoreTableFormat::Iceberg),
        location: table.location,
        schema: table.schema,
        partition_spec: table.partition_spec,
        sort_order: table.write_order,
        stage_create: table.stage_create,
        volume_ident,
        is_temporary: None,
    }
}

#[must_use]
pub fn from_schemas_list(schemas: Vec<RwObject<MetastoreSchema>>) -> ListNamespacesResponse {
    let namespaces = schemas
        .into_iter()
        .map(|schema| vec![schema.data.ident.schema])
        .collect::<Vec<Vec<String>>>();
    ListNamespacesResponse {
        next_page_token: None,
        namespaces: Some(namespaces),
    }
}

#[must_use]
pub fn to_table_commit(commit: CommitTable) -> MetastoreTableUpdate {
    MetastoreTableUpdate {
        requirements: commit.requirements,
        updates: commit.updates,
    }
}

#[must_use]
pub fn from_tables_list(tables: Vec<RwObject<MetastoreTable>>) -> ListTablesResponse {
    let identifiers = tables
        .into_iter()
        .map(|table| Identifier::new(&[table.data.ident.schema], &table.data.ident.table))
        .collect();
    ListTablesResponse {
        next_page_token: None,
        identifiers: Some(identifiers),
    }
}

#[derive(serde::Deserialize, Debug)]
pub struct GetConfigQuery {
    pub warehouse: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitTable {
    /// Assertions about the metadata that must be true to update the metadata
    pub requirements: Vec<TableRequirement>,
    /// Changes to the table metadata
    pub updates: Vec<IcebergTableUpdate>,
}
