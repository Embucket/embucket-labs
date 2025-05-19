use super::http::{http_req_with_headers, HttpErrorData};
use super::error::{HttpRequestError, HttpRequestSnafu};
use super::helpers::{login, refresh, get_set_cookie_name_value_map};
use api_structs::query::QueryCreatePayload;
use api_structs::auth::AuthResponse;
use http::{header, StatusCode, Method, HeaderMap, HeaderValue};
use snafu::ResultExt;
use std::net::SocketAddr;
use reqwest;
use serde::{de, de::Deserialize, de::DeserializeOwned};
use serde_json::json;
use api_structs::{auth::LoginPayload, query_context::QueryContext};
use cookie::Cookie;
use super::error::*;

pub struct AuthenticatedClient {
    pub client: reqwest::Client,
    pub addr: SocketAddr,
    pub access_token: String,
    pub refresh_token: String,
    pub session_id: Option<String>,
}

#[async_trait::async_trait]
pub trait AuthenticatedRequests {
    /// should login before run queries
    async fn login(
        &mut self, user: String, password: String
    ) -> HttpRequestResult<AuthResponse>;

    async fn refresh(&mut self) -> HttpRequestResult<AuthResponse>;

    async fn query<T: DeserializeOwned + Send>(
        &mut self,
        query: &str,
    ) -> HttpRequestResult<T>
    where
        Self: Sized;

    async fn generic_request<I, T>(
        &mut self,
        method: Method,
        url: &str,
        payload: &I,
    ) -> HttpRequestResult<T>
    where
        Self: Sized,
        I: serde::Serialize + Sync,
        T: serde::de::DeserializeOwned + Send;
}

impl AuthenticatedClient {
    fn set_tokens_from_auth_response (&mut self, headers: &HeaderMap, auth_response: &AuthResponse) {
        let from_set_cookies = get_set_cookie_name_value_map(&headers);
        self.refresh_token = from_set_cookies.get("refresh_token").unwrap();
        self.access_token = auth_response.access_token.clone();
    }

    fn set_session_id_from_response_headers(&mut self, headers: &HeaderMap) {
        let from_set_cookies = get_set_cookie_name_value_map(&headers);
        self.session_id = from_set_cookies.get("id");
    }

    async fn generic_request_no_refresh<I:serde::Serialize + Sync, T: serde::de::DeserializeOwned + Send>(
        &self,
        method: Method,
        url: &str,
        payload: &I,
    ) -> HttpRequestResult<T> 
    where
        Self: Sized
    {
        let Self { access_token, client, .. } = self;
        let res = http_req_with_headers::<T>(
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
        .map_err(HttpRequestError::from);

        match res {
            Ok((headers, resp_data)) => {
                self.set_session_id_from_response_headers(&headers);
                Ok(resp_data)
            }
            Err(err) => Err(err),
        }
    }
}

#[async_trait::async_trait]
impl AuthenticatedRequests for AuthenticatedClient {
    async fn login(&mut self, user: String, password: String) -> HttpRequestResult<AuthResponse> {
        let AuthenticatedClient { client, addr, .. } = self;

        let login_result = http_req_with_headers::<AuthResponse>(
            client,
            Method::POST,
            HeaderMap::from_iter(vec![(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )]),
            &format!("http://{addr}/ui/auth/login"),
            json!(LoginPayload {
                username: user,
                password: password,
            })
            .to_string(),
        )
        .await;

        match login_result {
            Ok((headers, auth_response)) => {
                self.set_tokens_from_auth_response(&headers, &auth_response);
                Ok(auth_response)
            }
            Err(err) => Err(HttpRequestError::from(err)),
        }
    }

    async fn refresh(&mut self) -> HttpRequestResult<AuthResponse> {
        let AuthenticatedClient { client, addr, refresh_token, .. } = self;

        let refresh_result = http_req_with_headers::<AuthResponse>(
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
        .await;

        match refresh_result {
            Ok((headers, auth_response)) => {
                self.set_tokens_from_auth_response(&headers, &auth_response);
                Ok(auth_response)
            }
            Err(err) => Err(HttpRequestError::from(err)),
        }
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
        url: &str,
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

}
