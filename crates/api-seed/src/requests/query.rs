#![allow(clippy::unwrap_used, clippy::expect_used)]
use api_ui::auth::error::AuthError;
use api_ui::auth::handlers::{create_jwt, get_claims_validate_jwt_token, jwt_claims};
use api_ui::auth::models::{AccountResponse};
use api_ui::queries::models::{QueryCreatePayload, QueryCreateResponse};
use api_ui::common::http_requests::{http_req_with_headers, TestHttpError};
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use serde_json::json;
use serde::{Serialize, de::DeserializeOwned};
use std::net::SocketAddr;
use reqwest;
use api_structs::auth::{LoginPayload, AuthResponse};
use snafu::prelude::*;

pub type AuthenticatedQueryResult<T> = std::result::Result<T, QueryRequestError>;

impl From<TestHttpError> for QueryRequestError {
    fn from(value: TestHttpError) -> Self {
        QueryRequestError::QueryRequest { message: value.error }
    }
}

#[derive(Debug, Snafu)]
pub enum QueryRequestError {
    #[snafu(display("Query error: {message}"))]
    QueryRequest { message: String },
}

pub struct QueryRequest {
    pub client: reqwest::Client,
    pub addr: SocketAddr,
    pub access_token: String,
    pub refresh_token: String,
}

pub trait AuthenticatedQueryRequest {
    async fn query<T: serde::de::DeserializeOwned>(
        &self,
        query: &str,
    ) -> AuthenticatedQueryResult<T>;
}

impl AuthenticatedQueryRequest for QueryRequest {
    async fn query<T>(
        &self,
        q: &str,
    ) ->  AuthenticatedQueryResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let QueryRequest { client, addr, access_token, refresh_token } = self;

        match query::<T>(client, addr, access_token, q).await {
            Ok(t) => Ok(t),
            Err(TestHttpError { status: StatusCode::UNAUTHORIZED, .. }) => {
                match refresh::<AuthResponse>(client, addr, refresh_token).await {
                    Ok((_,auth_resp)) => query::<T>(client, addr, &auth_resp.access_token, q)
                        .await
                        .map_err(QueryRequestError::from),
                    Err(err) => Err(err.into()),
                }
            },
            Err(err) => Err(err.into()),
        }
    }
}

async fn login<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
    username: &str,
    password: &str,
) -> Result<(HeaderMap, T), TestHttpError>
where
    T: serde::de::DeserializeOwned,
{
    http_req_with_headers::<T>(
        client,
        Method::POST,
        HeaderMap::from_iter(vec![(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )]),
        &format!("http://{addr}/ui/auth/login"),
        json!(LoginPayload {
            username: String::from(username),
            password: String::from(password),
        })
        .to_string(),
    )
    .await
}


async fn refresh<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
    refresh_token: &str,
) -> Result<(HeaderMap, T), TestHttpError>
where
    T: serde::de::DeserializeOwned,
{
    http_req_with_headers::<T>(
        client,
        Method::POST,
        HeaderMap::from_iter(vec![
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                header::COOKIE,
                HeaderValue::from_str(format!("refresh_token={refresh_token}").as_str())
                    .expect("Can't convert to HeaderValue"),
            ),
        ]),
        &format!("http://{addr}/ui/auth/refresh"),
        String::new(),
    )
    .await
}

async fn query<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
    access_token: &String,
    query: &str,
) -> Result<T, TestHttpError>
where
    T: serde::de::DeserializeOwned,
{
    http_req_with_headers::<T>(
        client,
        Method::POST,
        HeaderMap::from_iter(vec![
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                header::AUTHORIZATION,
                HeaderValue::from_str(format!("Bearer {access_token}").as_str())
                    .expect("Can't convert to HeaderValue"),
            ),
        ]),
        &format!("http://{addr}/ui/queries"),
        json!(QueryCreatePayload {
            worksheet_id: None,
            query: query.to_string(),
            context: None,
        })
        .to_string(),
    )
    .await
    .map(|(_, t)| t)
}
