use super::http::{http_req_with_headers, HttpErrorData};
use super::error::{HttpRequestError, HttpRequestSnafu};
use super::auth_helpers::{login, refresh};
use api_structs::query::QueryCreatePayload;
use api_structs::auth::AuthResponse;
use http::{header, StatusCode, Method, HeaderMap, HeaderValue};
use snafu::ResultExt;
use std::net::SocketAddr;
use reqwest;
use serde::{de, de::Deserialize, de::DeserializeOwned};
use serde_json::json;
use api_structs::auth::{LoginPayload, };
use super::error::*;


pub async fn query<T>(
    client: &reqwest::Client,
    addr: &SocketAddr,
    access_token: &str,
    query: &str,
) -> Result<(HeaderMap, T), HttpErrorData>
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
}


pub struct AuthenticatedClient {
    pub client: reqwest::Client,
    pub addr: SocketAddr,
    pub access_token: String,
    pub refresh_token: String,
}

#[async_trait::async_trait]
pub trait AuthenticatedRequests {
    /// should login before run queries
    async fn login(
        &mut self, user: String, password: String
    ) -> HttpRequestResult<AuthResponse>;

    async fn query<T: DeserializeOwned + Send>(
        &self,
        query: &str,
    ) -> HttpRequestResult<T>
    where
        Self: Sized;

    async fn generic_request<I, T>(
        method: Method,
        url: &String,
        payload: I,
    ) -> HttpRequestResult<T>
    where
        I: serde::Serialize,
        T: serde::de::DeserializeOwned;
}

#[async_trait::async_trait]
impl AuthenticatedRequests for AuthenticatedClient {
    async fn login(&mut self, user: String, password: String) -> HttpRequestResult<AuthResponse> {
        let AuthenticatedClient { client, addr, .. } = self;
        let login_result = login::<AuthResponse>(&client, &addr, &user, &password)
            .await
            .map(|(_, t)| t)
            .map_err(HttpRequestError::from);
        if let Ok(AuthResponse{ ref access_token, .. }) = login_result {
            self.access_token = access_token.clone();
        };
        login_result
    }


    async fn query<T: DeserializeOwned + Send>(
        &self,
        q: &str,
    ) -> HttpRequestResult<T>
    where
        Self: Sized
    {
        let AuthenticatedClient { client, addr, access_token, refresh_token } = &self;

        match query::<T>(client, addr, access_token, q).await {
            Ok((_, t)) => Ok(t),
            Err(HttpErrorData { status: StatusCode::UNAUTHORIZED, .. }) => {
                let refresh_result = refresh::<AuthResponse>(client, addr, refresh_token)
                    .await
                    .map_err(HttpRequestError::from);
                match refresh_result {
                    Ok((_, AuthResponse{ ref access_token, .. })) => {
                        query::<T>(client, addr, access_token, q)
                            .await
                            .map(|(_, t)| t)
                            .map_err(HttpRequestError::from)
                    }
                    Err(err) => Err(err),
                }

            },
            Err(err) => Err(HttpRequestError::from(err)),
        }
    }


    async fn generic_request<I:serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        url: &String,
        payload: I,
    ) -> HttpRequestResult<T> {
        http_req_with_headers::<T>(
            client,
            method,
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
            url,
            serde_json::to_string(&payload).context(SerializeSnafu)?,
        )
        .await
        .map(|(_, t)| t)
        .map_err(HttpRequestError::from)
    }
}
