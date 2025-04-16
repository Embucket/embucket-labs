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

// These are API wrappers for the metastore models

use embucket_metastore::models::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HTTPRwObject<T: ToSchema + Eq + Clone>(pub RwObject<T>);

impl<T: ToSchema + Eq + Clone> From<RwObject<T>> for HTTPRwObject<T> {
    fn from(rw_object: RwObject<T>) -> Self {
        Self(rw_object)
    }
}

impl<T: ToSchema + Eq + Clone> From<HTTPRwObject<T>> for RwObject<T> {
    fn from(http_rw_object: HTTPRwObject<T>) -> Self {
        http_rw_object.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HTTPRwObjectVec<T: ToSchema + Eq + Clone>(pub Vec<HTTPRwObject<T>>);

impl<T: ToSchema + Eq + Clone> From<Vec<RwObject<T>>> for HTTPRwObjectVec<T> {
    fn from(rw_objects: Vec<RwObject<T>>) -> Self {
        Self(rw_objects.into_iter().map(HTTPRwObject::from).collect())
    }
}

impl<T: ToSchema + Eq + Clone> From<HTTPRwObjectVec<T>> for Vec<RwObject<T>> {
    fn from(http_rw_objects: HTTPRwObjectVec<T>) -> Self {
        http_rw_objects.0.into_iter().map(RwObject::from).collect()
    }
}
