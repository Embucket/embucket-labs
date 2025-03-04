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

use chrono::Utc;
use std::collections::HashMap;

#[allow(clippy::implicit_hasher)]
pub fn update_properties_timestamps(properties: &mut HashMap<String, String>) {
    let utc_now = Utc::now();
    let utc_now_str = utc_now.to_rfc3339();
    properties.insert("created_at".to_string(), utc_now_str.clone());
    properties.insert("updated_at".to_string(), utc_now_str);
}

#[must_use]
pub fn get_default_properties() -> HashMap<String, String> {
    let mut properties = HashMap::new();
    update_properties_timestamps(&mut properties);
    properties
}
