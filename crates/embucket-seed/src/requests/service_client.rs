#![allow(clippy::expect_used)]
use super::error::{HttpRequestError, HttpRequestResult, InvalidHeaderValueSnafu, SerializeSnafu};
use super::helpers::get_set_cookie_name_value_map;
use super::http::http_req_with_headers;
use crate::external_models::{AuthResponse, LoginPayload, QueryCreatePayload};
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use reqwest;
use serde::de::DeserializeOwned;
use serde_json::json;
use snafu::ResultExt;
use std::fmt::Debug;
use std::net::SocketAddr;

/// A trait defining the interface for service clients that interact with the Embucket API.
///
/// This trait provides methods for authentication and making requests to the Embucket service.
#[async_trait::async_trait]
pub trait ServiceClient {
    fn addr(&self) -> SocketAddr;

    /// Authenticates with the Embucket service using the provided credentials.
    /// Must login before calling query or `generic_request` functions.
    ///
    /// # Arguments
    /// * `user` - The username for authentication
    /// * `password` - The password for authentication
    ///
    /// # Errors
    /// Returns `HttpRequestError` if authentication fails.
    async fn login(&mut self, user: &str, password: &str) -> HttpRequestResult<AuthResponse>;

    /// Refreshes the authentication token using the current refresh token.
    ///
    /// # Errors
    /// Returns `HttpRequestError` if token refresh fails.
    async fn refresh(&mut self) -> HttpRequestResult<AuthResponse>;

    /// Executes a SQL query against the Embucket service.
    ///
    /// # Type Parameters
    /// * `T` - The type to deserialize the response into
    ///
    /// # Errors
    /// Returns `HttpRequestError` if the query execution fails.
    async fn query<T: DeserializeOwned + Send + Debug>(
        &mut self,
        query: &str,
    ) -> HttpRequestResult<T>
    where
        Self: Sized;

    /// Sends a generic HTTP request to the Rest API.
    ///
    /// # Type Parameters
    /// * `I` - The type of the request payload (must be serializable)
    /// * `T` - The type to deserialize the response into
    ///
    /// # Errors
    /// Returns `HttpRequestError` if the request fails or returns an error status.
    async fn generic_request<I, T>(
        &mut self,
        method: Method,
        url: &str,
        payload: &I,
    ) -> HttpRequestResult<T>
    where
        I: serde::Serialize + Sync + Debug,
        T: serde::de::DeserializeOwned + Send + Debug;
}

/// A basic authentication client that implements the `ServiceClient` trait.
///
/// This client handles authentication token management and request/response
/// serialization for interacting with the Embucket API.
#[derive(Debug)]
pub struct BasicAuthClient {
    client: reqwest::Client,
    addr: SocketAddr,
    access_token: String,
    refresh_token: String,
    session_id: Option<String>,
}

impl BasicAuthClient {
    /// Creates a new `BasicAuthClient` with the specified server address.
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

    /// Updates the client's tokens from an authentication response.
    fn set_tokens_from_auth_response(&mut self, headers: &HeaderMap, auth_response: &AuthResponse) {
        let from_set_cookies = get_set_cookie_name_value_map(headers);
        if let Some(refresh_token) = from_set_cookies.get("refresh_token") {
            self.refresh_token.clone_from(refresh_token);
        }
        // as of recent changes to sessions, we expect session_id in response set-cookie
        self.set_session_id_from_response_headers(headers);
        self.access_token.clone_from(&auth_response.access_token);
    }

    /// Updates the session ID from response headers if present.
    fn set_session_id_from_response_headers(&mut self, headers: &HeaderMap) {
        let from_set_cookies = get_set_cookie_name_value_map(headers);
        if let Some(session_id) = from_set_cookies.get("session_id") {
            self.session_id = Some(session_id.clone());
        }
    }

    async fn generic_request_no_refresh<I, T>(
        &mut self,
        method: Method,
        url: &str,
        payload: &I,
    ) -> HttpRequestResult<T>
    where
        Self: Sized,
        I: serde::Serialize + Sync,
        T: serde::de::DeserializeOwned + Send,
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
            cookies.push(format!("session_id={session_id}"));
        }
        if !cookies.is_empty() {
            headers.insert(
                header::COOKIE,
                HeaderValue::from_str(cookies.join("; ").as_str())
                    .context(InvalidHeaderValueSnafu)?,
            );
        }

        tracing::trace!(?headers, "request headers");

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
                tracing::trace!(?headers, "response headers");
                self.set_session_id_from_response_headers(&headers);
                Ok(resp_data)
            }
            Err(err) => Err(err),
        }
    }
}

#[async_trait::async_trait]
impl ServiceClient for BasicAuthClient {
    fn addr(&self) -> SocketAddr {
        self.addr
    }

    async fn login(&mut self, user: &str, password: &str) -> HttpRequestResult<AuthResponse> {
        let Self { client, addr, .. } = self;

        tracing::trace!("login request");

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
                tracing::trace!(?headers, "login response headers");
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
        let headers = HeaderMap::from_iter(vec![
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                header::COOKIE,
                HeaderValue::from_str(format!("refresh_token={refresh_token}").as_str())
                    .expect("Can't convert to HeaderValue"),
            ),
        ]);

        tracing::trace!(?headers, "refresh request headers");

        let refresh_result = http_req_with_headers::<AuthResponse>(
            client,
            Method::POST,
            headers,
            &format!("http://{addr}/ui/auth/refresh"),
            String::new(),
        )
        .await;

        match refresh_result {
            Ok((headers, auth_response)) => {
                tracing::trace!(?headers, "refresh response headers");
                self.set_tokens_from_auth_response(&headers, &auth_response);
                Ok(auth_response)
            }
            Err(err) => Err(HttpRequestError::from(err)),
        }
    }

    // sets access_token at refresh if expired
    async fn query<T: DeserializeOwned + Send + Debug>(
        &mut self,
        query: &str,
    ) -> HttpRequestResult<T>
    where
        Self: Sized,
    {
        let url = format!("http://{}/ui/queries", self.addr);
        let query_payload = QueryCreatePayload {
            worksheet_id: None,
            async_exec: false,
            query: query.to_string(),
            context: None,
        };

        self.generic_request(Method::POST, &url, &query_payload)
            .await
    }

    #[tracing::instrument(level = "trace", skip(self), ret)]
    async fn generic_request<I, T>(
        &mut self,
        method: Method,
        url: &str,
        payload: &I,
    ) -> HttpRequestResult<T>
    where
        I: serde::Serialize + Sync + Debug,
        T: serde::de::DeserializeOwned + Send + Debug,
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
