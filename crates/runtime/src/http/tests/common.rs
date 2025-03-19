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

#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::http::{config::IceBucketWebConfig, make_icebucket_app};
use http::Method;
use icebucket_metastore::{IceBucketDatabase, IceBucketSchema, IceBucketVolume, SlateDBMetastore};
use reqwest::Response;
use serde_json::json;
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[allow(dead_code)]
#[derive(Debug)]
pub enum Entity {
    Volume(IceBucketVolume),
    Database(IceBucketDatabase),
    Schema(IceBucketSchema),
}

#[derive(Debug)]
pub enum Op {
    Create,
    List,
    Delete,
    Get,
    Update,
}

pub async fn create_server() -> SocketAddr {
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let metastore = SlateDBMetastore::new_in_memory().await;
    let app = make_icebucket_app(
        metastore,
        &IceBucketWebConfig {
            port: 3000,
            host: "0.0.0.0".to_string(),
            allow_origin: None,
            data_format: "json".to_string(),
            iceberg_catalog_url: "http://127.0.0.1:3000".to_string(),
        },
    )
    .unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}

pub async fn req(
    client: &reqwest::Client,
    method: Method,
    url: &String,
    payload: String,
) -> Result<reqwest::Response, reqwest::Error> {
    client
        .request(method, url)
        .header("Content-Type", "application/json")
        .body(payload)
        .send()
        .await
}

fn ui_op_endpoint(addr: SocketAddr, t: &Entity, op: &Op) -> String {
    match t {
        Entity::Volume(vol) => match op {
            Op::Create | Op::List => format!("http://{addr}/ui/volumes"),
            Op::Delete | Op::Get | Op::Update => format!("http://{addr}/ui/volumes/{}", vol.ident),
        },
        Entity::Database(db) => match op {
            Op::Create | Op::List => format!("http://{addr}/ui/databases"),
            Op::Delete | Op::Get | Op::Update => format!("http://{addr}/ui/databases/{}", db.ident),
        },
        Entity::Schema(sc) => match op {
            Op::Create | Op::List => {
                format!("http://{addr}/ui/databases/{}/schemas", sc.ident.database)
            }
            Op::Delete | Op::Get | Op::Update => format!(
                "http://{addr}/ui/databases/{}/schemas/{}",
                sc.ident.database, sc.ident.schema
            ),
        },
    }
}

// op list expects empty entity - stub
// op update require two entities: t_from ,t
pub async fn ui_test_op(addr: SocketAddr, op: Op, t_from: Option<&Entity>, t: &Entity) -> Response {
    let ui_url = match t_from {
        Some(t_from) => ui_op_endpoint(addr, t_from, &op),
        None => ui_op_endpoint(addr, t, &op),
    };
    let client = reqwest::Client::new();
    let payload = match t {
        Entity::Volume(vol) => json!(vol).to_string(),
        Entity::Database(db) => json!(db).to_string(),
        Entity::Schema(sc) => json!(sc).to_string(),
    };
    match op {
        Op::Create => req(&client, Method::POST, &ui_url, payload).await.unwrap(),
        Op::Delete => req(&client, Method::DELETE, &ui_url, payload)
            .await
            .unwrap(),
        Op::Get | Op::List => req(&client, Method::GET, &ui_url, payload).await.unwrap(),
        Op::Update => req(&client, Method::PUT, &ui_url, payload).await.unwrap(),
    }
}
