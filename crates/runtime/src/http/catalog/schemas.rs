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

use iceberg_rest_catalog::models::{
    CommitTableRequest, CreateNamespaceRequest, CreateNamespaceResponse, CreateTableRequest,
    GetNamespaceResponse, ListNamespacesResponse, ListTablesResponse,
};
use iceberg_rust_spec::identifier::Identifier;
use icebucket_metastore::{
    IceBucketSchema, IceBucketSchemaIdent, IceBucketTable, IceBucketTableCreateRequest,
    IceBucketTableFormat, IceBucketTableIdent, IceBucketTableUpdate, IceBucketVolumeIdent,
    RwObject,
};

#[must_use]
pub fn to_schema(request: CreateNamespaceRequest, db: String) -> IceBucketSchema {
    IceBucketSchema {
        ident: IceBucketSchemaIdent {
            schema: request
                .namespace
                .first()
                .unwrap_or(&String::new())
                .to_string(),
            database: db,
        },
        properties: request.properties,
    }
}

#[must_use]
pub fn from_schema(schema: IceBucketSchema) -> CreateNamespaceResponse {
    CreateNamespaceResponse {
        namespace: vec![schema.ident.database],
        properties: schema.properties,
    }
}

#[must_use]
pub fn from_get_schema(schema: IceBucketSchema) -> GetNamespaceResponse {
    GetNamespaceResponse {
        namespace: vec![schema.ident.database],
        properties: schema.properties,
    }
}

#[must_use]
pub fn to_create_table(
    table: CreateTableRequest,
    table_ident: IceBucketTableIdent,
    volume_ident: Option<IceBucketVolumeIdent>,
) -> IceBucketTableCreateRequest {
    IceBucketTableCreateRequest {
        ident: table_ident,
        properties: table.properties,
        format: Some(IceBucketTableFormat::Iceberg),
        location: table.location,
        schema: *table.schema,
        partition_spec: table.partition_spec.map(|spec| *spec),
        sort_order: table.write_order.map(|order| *order),
        stage_create: table.stage_create,
        volume_ident,
        is_temporary: None,
    }
}

#[must_use]
pub fn from_schemas_list(schemas: Vec<RwObject<IceBucketSchema>>) -> ListNamespacesResponse {
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
pub fn to_table_commit(commit: CommitTableRequest) -> IceBucketTableUpdate {
    IceBucketTableUpdate {
        requirements: commit.requirements,
        updates: commit.updates,
    }
}

#[must_use]
pub fn from_tables_list(tables: Vec<RwObject<IceBucketTable>>) -> ListTablesResponse {
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
