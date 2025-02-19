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

use nexus::router::create_app;
use nexus::state::AppState;
use nexus::repository::InMemoryStorageProfileRepository;
use nexus::service::StorageProfileServiceImpl;
use axum::Router;


fn create_app() -> Router {
    let repository = Arc::new(InMemoryStorageProfileRepository::new());
    let storage_profile_service = Arc::new(StorageProfileServiceImpl::new(repository));
    let app_state = AppState::new(storage_profile_service);
    create_app(app_state)
}


#[tokio::test]
async fn test_create_storage_profile() {
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/storage-profile")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}