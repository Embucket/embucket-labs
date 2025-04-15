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

use crate::error::{MetastoreError, MetastoreResult};
use iceberg_rust::{
    catalog::commit::{TableRequirement, TableUpdate},
    spec::table_metadata::TableMetadata,
};
use iceberg_rust_spec::{
    partition::PartitionSpec, schema::Schema, sort::SortOrder, spec::identifier::Identifier,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};
use utoipa::ToSchema;
use validator::Validate;

use super::{IceBucketSchemaIdent, IceBucketVolumeIdent};

#[derive(Validate, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
/// A table identifier
pub struct IceBucketTableIdent {
    #[validate(length(min = 1))]
    /// The name of the table
    pub table: String,
    #[validate(length(min = 1))]
    /// The schema the table belongs to
    pub schema: String,
    #[validate(length(min = 1))]
    /// The database the table belongs to
    pub database: String,
}

impl IceBucketTableIdent {
    #[must_use]
    pub fn new(database: &str, schema: &str, table: &str) -> Self {
        Self {
            table: table.to_string(),
            schema: schema.to_string(),
            database: database.to_string(),
        }
    }
    #[must_use]
    pub fn to_iceberg_ident(&self) -> Identifier {
        let namespace = vec![self.schema.clone()];
        Identifier::new(&namespace, &self.table)
    }
}

impl From<IceBucketTableIdent> for IceBucketSchemaIdent {
    fn from(ident: IceBucketTableIdent) -> Self {
        Self {
            database: ident.database,
            schema: ident.schema,
        }
    }
}

impl Display for IceBucketTableIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.database, self.schema, self.table)
    }
}

#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, utoipa::ToSchema, strum::EnumString,
)]
#[serde(rename_all = "kebab-case")]
pub enum IceBucketTableFormat {
    /*
    Avro,
    Orc,
    Delta,
    Json,
    Csv,*/
    Parquet,
    Iceberg,
}

impl From<String> for IceBucketTableFormat {
    fn from(value: String) -> Self {
        match value.to_lowercase().as_str() {
            "parquet" => Self::Parquet,
            _ => Self::Iceberg,
        }
    }
}

/*#[derive(Validate, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct IceBucketSimpleSchema {
    pub fields: Vec<NestedFieldRef>,
    pub schema_id: Option<i32>,
}

impl TryFrom<IceBucketSimpleSchema> for Schema {
    type Error = MetastoreError;
    fn try_from(schema: IceBucketSimpleSchema) -> MetastoreResult<Self> {
        let mut builder = Self::builder();
        builder = builder.with_fields(schema.fields);
        if let Some(schema_id) = schema.schema_id {
            builder = builder.with_schema_id(schema_id);
        }
        builder.build()
            .context(metastore_error::IcebergSnafu)
    }
}

type SimpleOrIcebergSchema = Either<IceBucketSimpleSchema, Schema>;*/

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct IceBucketTable {
    pub ident: IceBucketTableIdent,
    pub metadata: TableMetadata,
    pub metadata_location: String,
    pub properties: HashMap<String, String>,
    pub volume_ident: Option<IceBucketVolumeIdent>,
    pub volume_location: Option<String>,
    pub is_temporary: bool,
    pub format: IceBucketTableFormat,
}

/*impl PartialSchema for IceBucketTable {
    fn schema() -> openapi::RefOr<openapi::schema::Schema> {

        let table_metadata_schema = openapi::ObjectBuilder::new()
            .property("format_version", openapi::ObjectBuilder::new()
                .schema_type(openapi::Type::Integer)
                .format(Some(openapi::SchemaFormat::KnownFormat(openapi::KnownFormat::Int32)))
                .build()
            )
            .property(
                "table_uuid",
                openapi::Object::with_type(openapi::Type::String))
            .property("name", openapi::schema::String::default())
            .property("schema_id", openapi::schema::Integer::default())
            .property("current_schema_id", openapi::schema::Integer::default())
            .property("default_partition_spec_id", openapi::schema::Integer::default())
            .property("default_sort_order_id", openapi::schema::Integer::default())
            .property("last_partition_id", openapi::schema::Integer::default())
            .property("last_column_id", openapi::schema::Integer::default())
            .property("refs", openapi::schema::Object::default())
            .property("properties", utoipa_schema::Map::default())
            .property("schema", openapi::schema::Object::default())
            .property("partition_spec", openapi::schema::Object::default())
            .property("sort_order", openapi::schema::Object::default())
            .build();
        openapi::ObjectBuilder::default()
            .property("ident", IceBucketTableIdent::schema())
            .property("metadata", table_metadata_schema)
            .property("metadata_location", openapi::schema::String::default())
            .property("properties", utoipa_schema::Map::default())
            .build()
    }
}
impl ToSchema for IceBucketTable {}*/

#[derive(Validate, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct IceBucketTableCreateRequest {
    #[validate(nested)]
    pub ident: IceBucketTableIdent,
    pub properties: Option<HashMap<String, String>>,
    pub format: Option<IceBucketTableFormat>,

    pub location: Option<String>,
    //pub schema: SimpleOrIcebergSchema,
    pub schema: Schema,
    pub partition_spec: Option<PartitionSpec>,
    pub sort_order: Option<SortOrder>,
    pub stage_create: Option<bool>,
    pub volume_ident: Option<IceBucketVolumeIdent>,
    pub is_temporary: Option<bool>,
}

/*fn type_schema() -> (String, openapi::RefOr<openapi::schema::Schema>) {
    let primitive_type = openapi::OneOfBuilder::new()
        .item(openapi::ObjectBuilder::new()
            .schema_type(openapi::schema::SchemaType::new(openapi::schema::Type::String))
            .enum_values(Some(vec!["boolean", "int", "long", "float", "double", "date", "time", "timestamp", "timestamptz", "string", "uuid", "binary"]))
        )
        .item(openapi::ObjectBuilder::new()
            .schema_type(openapi::schema::SchemaType::new(openapi::schema::Type::Object))
            .property("precision", openapi::ObjectBuilder::new()
            .schema_type(openapi::schema::SchemaType::new(openapi::schema::Type::Integer))
            .build())
            .property("scale", openapi::schema::Type::Integer)
        )
        .item(openapi::ObjectBuilder::new()
            .schema_type(openapi::schema::SchemaType::new(openapi::schema::Type::Integer)))
        .build();
    let struct_type = openapi::RefOr::Ref(openapi::Ref::builder().ref_location_from_schema_name("StructType".to_string()).build());
    let list_type = openapi::ObjectBuilder::new()
        .property("element_id", openapi::ObjectBuilder::new()
            .schema_type(openapi::schema::SchemaType::new(openapi::schema::Type::Integer))
            .build()
        )
        .property("element_required", openapi::ObjectBuilder::new()
            .schema_type(openapi::schema::SchemaType::new(openapi::schema::Type::Boolean))
            .build()
        )
        .property("element", openapi::RefOr::Ref(openapi::Ref::builder().ref_location_from_schema_name("Type".to_string()).build()))
        .build();
    let map_type = openapi::ObjectBuilder::new()
        .property("key_id", openapi::ObjectBuilder::new()
            .schema_type(openapi::schema::SchemaType::new(openapi::schema::Type::Integer))
            .build()
        )
        .property("key", openapi::RefOr::Ref(openapi::Ref::builder().ref_location_from_schema_name("Type".to_string()).build()))
        .property("value_id", openapi::ObjectBuilder::new()
            .schema_type(openapi::schema::SchemaType::new(openapi::schema::Type::Integer))
            .build()
        )
        .property("value", openapi::RefOr::Ref(openapi::Ref::builder().ref_location_from_schema_name("Type".to_string()).build()))
        .property("value_required", openapi::ObjectBuilder::new()
            .schema_type(openapi::schema::SchemaType::new(openapi::schema::Type::Boolean))
            .build()
        )
        .build();
    let one_of = openapi::OneOf::builder()
        .item(primitive_type.into())
        .item(struct_type)
        .item(list_type)
        .item(map_type);
    ("Type".to_string(), one_of.into())
}

impl ToSchema for IceBucketTableCreateRequest {}
impl PartialSchema for IceBucketTableCreateRequest {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {

        let
        let mut type_schema = openapi::OneOfBuilder::new()
            .item(primitive_type)


        let mut struct_field_type = openapi::OneOfBuilder::new()
            .item(primitive_type)
        let struct_field = openapi::ObjectBuilder::new()
            .property("id", )
    }
}*/

#[derive(ToSchema, Deserialize, Serialize)]
enum MyPrimitive {
    Int,
    Str,
    Decimal { precision: u32, scale: u32 },
}

#[derive(ToSchema, Deserialize, Serialize)]
struct MyMap {
    key: Box<TypeEnum>,
    value: Box<TypeEnum>,
}

#[derive(ToSchema, Deserialize, Serialize)]
struct MyList {
    element: Box<TypeEnum>,
}

#[derive(ToSchema, Deserialize, Serialize)]
struct MyStruct {
    fields: Vec<MyStructField>,
}

#[derive(ToSchema, Deserialize, Serialize)]
struct MyStructField {
    #[serde(rename = "type")]
    field_type: TypeEnum,
}

#[derive(ToSchema, Deserialize, Serialize)]
enum TypeEnum {
    Struct(MyStruct),
    List(MyList),
    Map(MyMap),
    Primitive(MyPrimitive),
}

#[derive(ToSchema, Deserialize, Serialize)]
struct MySchema {
    #[serde(flatten)]
    fields: MyStruct,
}
/*impl TryFrom<IceBucketTableCreateRequest> for iceberg::TableCreation {
    type Error = MetastoreError;

    fn try_from(schema: IceBucketTableCreateRequest) -> MetastoreResult<Self> {
        let mut properties = schema.properties.unwrap_or_default();
        let utc_now = Utc::now();
        let utc_now_str = utc_now.to_rfc3339();
        properties.insert("created_at".to_string(), utc_now_str.clone());
        properties.insert("updated_at".to_string(), utc_now_str);

        let table_schema = match schema.schema {
            Either::Left(simple_schema) => {
                Schema::try_from(simple_schema)?
            }
            Either::Right(schema) => schema,
        };

        Ok(Self {
            name: schema.ident.table,
            location: schema.location,
            schema: table_schema,
            partition_spec: schema.partition_spec.map(std::convert::Into::into),
            sort_order: schema.write_order,
            properties,
        })
    }
}*/

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Config {
    pub defaults: HashMap<String, String>,
    pub overrides: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IceBucketTableUpdate {
    /// Commit will fail if the requirements are not met.
    pub requirements: Vec<TableRequirement>,
    /// The updates of the table.
    pub updates: Vec<TableUpdate>,
}

pub struct TableRequirementExt(TableRequirement);

impl From<TableRequirement> for TableRequirementExt {
    fn from(requirement: TableRequirement) -> Self {
        Self(requirement)
    }
}

impl TableRequirementExt {
    #[must_use]
    pub const fn new(requirement: TableRequirement) -> Self {
        Self(requirement)
    }

    #[must_use]
    pub const fn inner(&self) -> &TableRequirement {
        &self.0
    }

    pub fn assert(&self, metadata: &TableMetadata, exists: bool) -> MetastoreResult<()> {
        match self.inner() {
            TableRequirement::AssertCreate => {
                if exists {
                    return Err(MetastoreError::TableDataExists {
                        location: metadata.location.to_string(),
                    });
                }
            }
            TableRequirement::AssertTableUuid { uuid } => {
                if &metadata.table_uuid != uuid {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table uuid does not match".to_string(),
                    });
                }
            }
            TableRequirement::AssertCurrentSchemaId { current_schema_id } => {
                if metadata.current_schema_id != *current_schema_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table current schema id does not match".to_string(),
                    });
                }
            }
            TableRequirement::AssertDefaultSortOrderId {
                default_sort_order_id,
            } => {
                if metadata.default_sort_order_id != *default_sort_order_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table default sort order id does not match".to_string(),
                    });
                }
            }
            TableRequirement::AssertRefSnapshotId { r#ref, snapshot_id } => {
                if let Some(snapshot_id) = snapshot_id {
                    let snapshot_ref = metadata.refs.get(r#ref).ok_or_else(|| {
                        MetastoreError::TableRequirementFailed {
                            message: "Table ref not found".to_string(),
                        }
                    })?;
                    if snapshot_ref.snapshot_id != *snapshot_id {
                        return Err(MetastoreError::TableRequirementFailed {
                            message: "Table ref snapshot id does not match".to_string(),
                        });
                    }
                } else if metadata.refs.contains_key(r#ref) {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table ref already exists".to_string(),
                    });
                }
            }
            TableRequirement::AssertDefaultSpecId { default_spec_id } => {
                // ToDo: Harmonize the types of default_spec_id
                if metadata.default_spec_id != *default_spec_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table default spec id does not match".to_string(),
                    });
                }
            }
            TableRequirement::AssertLastAssignedPartitionId {
                last_assigned_partition_id,
            } => {
                if metadata.last_partition_id != *last_assigned_partition_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table last assigned partition id does not match".to_string(),
                    });
                }
            }
            TableRequirement::AssertLastAssignedFieldId {
                last_assigned_field_id,
            } => {
                if &metadata.last_column_id != last_assigned_field_id {
                    return Err(MetastoreError::TableRequirementFailed {
                        message: "Table last assigned field id does not match".to_string(),
                    });
                }
            }
        };
        Ok(())
    }
}
