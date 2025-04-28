#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::http::ui::databases::models::DatabaseCreatePayload;
use crate::http::ui::volumes::models::VolumeCreatePayload;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use serde_json::json;
use std::net::SocketAddr;

#[derive(Debug)]
pub struct TestHttpError {
    pub method: Method,
    pub url: String,
    pub headers: HeaderMap<HeaderValue>,
    pub status: StatusCode,
    pub body: String,
    pub error: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum Entity {
    Volume(VolumeCreatePayload),
    Database(DatabaseCreatePayload),
    //Schema(SchemaCreatePayload),
}

#[derive(Debug)]
pub enum Op {
    Create,
    List,
    Delete,
    Get,
    Update,
}

pub async fn req(
    client: &reqwest::Client,
    method: Method,
    url: &String,
    payload: String,
) -> Result<reqwest::Response, reqwest::Error> {
    let res = client
        .request(method.clone(), url)
        .header("Content-Type", "application/json")
        .body(payload)
        .send()
        .await;

    eprintln!("req: {method} {url}, {res:?}");

    res
}

pub async fn http_req<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    method: Method,
    url: &String,
    payload: String,
) -> Result<T, TestHttpError> {
    let res = client
        .request(method.clone(), url)
        .header("Content-Type", "application/json")
        .body(payload)
        .send()
        .await;

    let response = res.unwrap();
    if response.status() == StatusCode::OK {
        let headers = response.headers().clone();
        let status = response.status();
        let text = response.text().await.expect("Failed to get response text");
        if text.is_empty() {
            // If no actual type retuned we emulate unit, by "null" value in json
            Ok(serde_json::from_str::<T>("null").expect("Failed to parse response"))
        } else {
            let json = serde_json::from_str::<T>(&text);
            match json {
                Ok(json) => Ok(json),
                Err(err) => {
                    // Normally we don't expect error here, and only have http related error to return
                    Err(TestHttpError {
                        method,
                        url: url.clone(),
                        headers,
                        status,
                        body: text,
                        error: err.to_string(),
                    })
                }
            }
        }
    } else {
        let error = response
            .error_for_status_ref()
            .expect_err("Expected error, http code not OK");
        // Return custom error as reqwest error has no body contents
        Err(TestHttpError {
            method,
            url: url.clone(),
            headers: response.headers().clone(),
            status: response.status(),
            body: response.text().await.expect("Failed to get response text"),
            error: format!("{error:?}"),
        })
    }
}

fn ui_op_endpoint(addr: SocketAddr, t: &Entity, op: &Op) -> String {
    match t {
        Entity::Volume(vol) => match op {
            Op::Create | Op::List => format!("http://{addr}/ui/volumes"),
            Op::Delete | Op::Get | Op::Update => {
                format!("http://{addr}/ui/volumes/{}", vol.data.name)
            }
        },
        Entity::Database(db) => match op {
            Op::Create | Op::List => format!("http://{addr}/ui/databases"),
            Op::Delete | Op::Get | Op::Update => {
                format!("http://{addr}/ui/databases/{}", db.data.name)
            }
        },
        // Entity::Schema(sc) => match op {
        //     Op::Create | Op::List => {
        //         format!("http://{addr}/ui/databases/{}/schemas", sc.data.database)
        //     }
        //     Op::Delete | Op::Get | Op::Update => format!(
        //         "http://{addr}/ui/databases/{}/schemas/{}",
        //         sc.data.database, sc.data.name
        //     ),
        // },
    }
}

// op list expects empty entity - stub
// op update require two entities: t_from ,t
pub async fn ui_test_op(
    addr: SocketAddr,
    op: Op,
    t_from: Option<&Entity>,
    t: &Entity,
) -> reqwest::Response {
    let ui_url = match t_from {
        Some(t_from) => ui_op_endpoint(addr, t_from, &op),
        None => ui_op_endpoint(addr, t, &op),
    };
    let client = reqwest::Client::new();
    let payload = match t {
        Entity::Volume(vol) => json!(vol).to_string(),
        Entity::Database(db) => json!(db).to_string(),
        //Entity::Schema(sc) => json!(sc).to_string(),
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
