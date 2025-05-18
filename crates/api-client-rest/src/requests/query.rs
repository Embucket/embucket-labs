use super::http::{http_req_with_headers, HttpErrorData};
use super::error::QueryRequestSnafu;
use super::auth::{login, refresh};
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


pub struct QueryRequest {
    pub client: reqwest::Client,
    pub addr: SocketAddr,
    pub access_token: String,
    pub refresh_token: String,
}

#[async_trait::async_trait]
pub trait AuthenticatedQueryRequest {
    /// should login before run queries
    async fn login(
        &mut self, user: String, password: String
    ) -> AuthenticatedQueryResult<AuthResponse>;

    async fn query<T: DeserializeOwned + Send>(
        &self,
        query: &str,
    ) -> AuthenticatedQueryResult<T>
    where
        Self: Sized;
}

#[async_trait::async_trait]
impl AuthenticatedQueryRequest for QueryRequest {
    async fn login(&mut self, user: String, password: String) -> AuthenticatedQueryResult<AuthResponse> {
        let QueryRequest { client, addr, .. } = self;
        let login_result = login::<AuthResponse>(&client, &addr, &user, &password)
            .await
            .map(|(_, t)| t)
            .map_err(|err| err.error)
            .context(QueryRequestSnafu);
        if let Ok(AuthResponse{ ref access_token, .. }) = login_result {
            self.access_token = access_token.clone();
        };
        login_result
    }


    async fn query<T: DeserializeOwned + Send>(
        &self,
        q: &str,
    ) -> AuthenticatedQueryResult<T>
    where
        Self: Sized
    {
        let QueryRequest { client, addr, access_token, refresh_token } = &self;

        match query::<T>(client, addr, access_token, q).await {
            Ok((_, t)) => Ok(t),
            Err(HttpErrorData { status: StatusCode::UNAUTHORIZED, .. }) => {
                let refresh_result = refresh::<AuthResponse>(client, addr, refresh_token)
                    .await
                    .map_err(|err| err.error)
                    .context(QueryRequestSnafu);
                match refresh_result {
                    Ok((_, AuthResponse{ ref access_token, .. })) => {
                        query::<T>(client, addr, access_token, q)
                            .await
                            .map(|(_, t)| t)
                            .map_err(|err|err.error)
                            .context(QueryRequestSnafu)
                    }
                    Err(err) => Err(err),
                }

            },
            Err(err) => Err(QueryRequestError::QueryRequest{ source: err.error}),
        }
    }
}
