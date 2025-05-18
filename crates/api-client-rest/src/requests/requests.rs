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
use api_structs::{auth::LoginPayload, query_context::QueryContext};
use super::error::*;

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
        &mut self,
        query: &str,
    ) -> HttpRequestResult<T>
    where
        Self: Sized;

    async fn generic_request<I, T>(
        &mut self,
        method: Method,
        url: &String,
        payload: &I,
    ) -> HttpRequestResult<T>
    where
        Self: Sized,
        I: serde::Serialize + Sync,
        T: serde::de::DeserializeOwned + Send;

    async fn generic_request_no_refresh<I, T>(
        &self,
        method: Method,
        url: &String,
        payload: &I,
    ) -> HttpRequestResult<T>
    where
        Self: Sized,
        I: serde::Serialize + Sync,
        T: serde::de::DeserializeOwned + Send;
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

    // sets access_token at refresh if expired
    async fn query<T: DeserializeOwned + Send>(
        &mut self,
        query: &str,
    ) -> HttpRequestResult<T>
    where
        Self: Sized
    {
        let url = format!("http://{}/ui/queries", self.addr);
        let query_payload = QueryCreatePayload {
            worksheet_id: None,
            query: query.to_string(),
            context: None,
        };

        match self.generic_request_no_refresh(Method::POST, &url, &query_payload).await {
            Ok(t) => Ok(t),
            Err(HttpRequestError::HttpRequest { status: StatusCode::UNAUTHORIZED, .. }) => {
                let refresh_result = refresh::<AuthResponse>(
                    &self.client, &self.addr, &self.refresh_token
                ).await.map_err(HttpRequestError::from);

                match refresh_result {
                    Ok((_, AuthResponse{ ref access_token, .. })) => {
                        self.access_token = access_token.clone();
                        self.generic_request_no_refresh(Method::POST, &url, &query_payload).await
                    }
                    Err(err) => Err(err),
                }

            },
            Err(err) => Err(err),
        }
    }

    async fn generic_request<I, T>(
        &mut self,
        method: Method,
        url: &String,
        payload: &I,
    ) -> HttpRequestResult<T>
    where
        Self: Sized,
        I: serde::Serialize + Sync,
        T: serde::de::DeserializeOwned + Send
    {
        match self.generic_request_no_refresh(method.clone(), &url, payload).await {
            Ok(t) => Ok(t),
            Err(HttpRequestError::HttpRequest { status: StatusCode::UNAUTHORIZED, .. }) => {
                let refresh_result = refresh::<AuthResponse>(
                    &self.client, &self.addr, &self.refresh_token
                ).await.map_err(HttpRequestError::from);

                match refresh_result {
                    Ok((_, AuthResponse{ ref access_token, .. })) => {
                        self.access_token = access_token.clone();
                        self.generic_request_no_refresh(method, &url, payload).await
                    }
                    Err(err) => Err(err),
                }

            },
            Err(err) => Err(err),
        }
    }

    async fn generic_request_no_refresh<I:serde::Serialize + Sync, T: serde::de::DeserializeOwned + Send>(
        &self,
        method: Method,
        url: &String,
        payload: &I,
    ) -> HttpRequestResult<T> 
    where
        Self: Sized
    {
        let Self { access_token, client, .. } = self;
        http_req_with_headers::<T>(
            &client,
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
