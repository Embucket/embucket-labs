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

use chrono::NaiveDateTime;
use control_plane::models;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateWarehouseRequest {
    pub prefix: String,
    pub name: String,
    pub storage_profile_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Warehouse {
    pub id: Uuid,
    pub prefix: String,
    pub name: String,
    pub location: String,
    pub storage_profile_id: Uuid,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl From<CreateWarehouseRequest> for models::WarehouseCreateRequest {
    fn from(request: CreateWarehouseRequest) -> Self {
        Self {
            prefix: request.prefix,
            name: request.name,
            storage_profile_id: request.storage_profile_id,
        }
    }
}

impl From<models::Warehouse> for Warehouse {
    fn from(warehouse: models::Warehouse) -> Self {
        Self {
            id: warehouse.id,
            prefix: warehouse.prefix,
            name: warehouse.name,
            location: warehouse.location,
            storage_profile_id: warehouse.storage_profile_id,
            created_at: warehouse.created_at,
            updated_at: warehouse.updated_at,
        }
    }
}
