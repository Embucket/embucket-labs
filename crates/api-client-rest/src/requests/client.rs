#![allow(clippy::expect_used)]
use super::error::{HttpRequestError, HttpRequestResult, InvalidHeaderValueSnafu, SerializeSnafu};
use super::helpers::get_set_cookie_name_value_map;
use super::http::http_req_with_headers;
use api_structs::auth::{AuthResponse, LoginPayload};
use api_structs::query::QueryCreatePayload;
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use reqwest;
use serde::de::DeserializeOwned;
use serde_json::json;
use snafu::ResultExt;
use std::net::SocketAddr;

pub struct AuthenticatedClient {
    client: reqwest::Client,
    addr: SocketAddr,
    access_token: String,
    refresh_token: String,
    session_id: Option<String>,
}

#[async_trait::async_trait]
pub trait AuthenticatedRequests {
    fn addr(&self) -> SocketAddr;

    /// should login before run queries
    async fn login(&mut self, user: &str, password: &str) -> HttpRequestResult<AuthResponse>;

    async fn refresh(&mut self) -> HttpRequestResult<AuthResponse>;

    async fn query<T: DeserializeOwned + Send>(&mut self, query: &str) -> HttpRequestResult<T>
    where
        Self: Sized;

    async fn generic_request<I, T>(
        &mut self,
        method: Method,
        url: &str,
        payload: &I,
    ) -> HttpRequestResult<T>
    where
        I: serde::Serialize + Sync,
        T: serde::de::DeserializeOwned + Send;
}

impl AuthenticatedClient {
    #[must_use]
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            client: reqwest::Client::new(),
            addr,
            access_token: String::new(),
            refresh_token: String::new(),
            session_id: None,
        }
    }

    fn set_tokens_from_auth_response(&mut self, headers: &HeaderMap, auth_response: &AuthResponse) {
        let from_set_cookies = get_set_cookie_name_value_map(headers);
        if let Some(refresh_token) = from_set_cookies.get("refresh_token") {
            self.refresh_token.clone_from(refresh_token);
        }
        self.access_token.clone_from(&auth_response.access_token);
    }

    fn set_session_id_from_response_headers(&mut self, headers: &HeaderMap) {
        let from_set_cookies = get_set_cookie_name_value_map(headers);
        self.session_id = from_set_cookies.get("id").cloned();
    }

    async fn generic_request_no_refresh<
        I: serde::Serialize + Sync,
        T: serde::de::DeserializeOwned + Send,
    >(
        &mut self,
        method: Method,
        url: &str,
        payload: &I,
    ) -> HttpRequestResult<T>
    where
        Self: Sized,
    {
        let Self {
            access_token,
            refresh_token,
            client,
            ..
        } = self;

        let mut headers = HeaderMap::from_iter(vec![
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                header::AUTHORIZATION,
                HeaderValue::from_str(format!("Bearer {access_token}").as_str())
                    .expect("Can't convert to HeaderValue"),
            ),
        ]);

        // prepare cookies
        let mut cookies = Vec::new();
        if !refresh_token.is_empty() {
            cookies.push(format!("refresh_token={refresh_token}"));
        }
        if let Some(session_id) = &self.session_id {
            cookies.push(format!("id={session_id}"));
        }
        if !cookies.is_empty() {
            headers.insert(
                header::COOKIE,
                HeaderValue::from_str(cookies.join(";").as_str())
                    .context(InvalidHeaderValueSnafu)?,
            );
        }

        let res = http_req_with_headers::<T>(
            client,
            method,
            headers,
            url,
            serde_json::to_string(&payload).context(SerializeSnafu)?,
        )
        .await
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
    fn addr(&self) -> SocketAddr {
        self.addr
    }

    async fn login(&mut self, user: &str, password: &str) -> HttpRequestResult<AuthResponse> {
        let Self { client, addr, .. } = self;

        let login_result = http_req_with_headers::<AuthResponse>(
            client,
            Method::POST,
            HeaderMap::from_iter(vec![(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )]),
            &format!("http://{addr}/ui/auth/login"),
            json!(LoginPayload {
                username: user.to_string(),
                password: password.to_string(),
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
        let Self {
            client,
            addr,
            refresh_token,
            ..
        } = self;

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
    async fn query<T: DeserializeOwned + Send>(&mut self, query: &str) -> HttpRequestResult<T>
    where
        Self: Sized,
    {
        let url = format!("http://{}/ui/queries", self.addr);
        let query_payload = QueryCreatePayload {
            worksheet_id: None,
            query: query.to_string(),
            context: None,
        };

        self.generic_request(Method::POST, &url, &query_payload)
            .await
    }

    async fn generic_request<I, T>(
        &mut self,
        method: Method,
        url: &str,
        payload: &I,
    ) -> HttpRequestResult<T>
    where
        I: serde::Serialize + Sync,
        T: serde::de::DeserializeOwned + Send,
    {
        match self
            .generic_request_no_refresh(method.clone(), url, payload)
            .await
        {
            Ok(t) => Ok(t),
            Err(HttpRequestError::HttpRequest {
                status: StatusCode::UNAUTHORIZED,
                ..
            }) => {
                let _refresh_resp = self.refresh().await?;
                self.generic_request_no_refresh(method, url, payload).await
            }
            Err(err) => Err(err),
        }
    }
}
