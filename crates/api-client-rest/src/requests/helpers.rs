use super::error::{HttpRequestError, HttpRequestResult, SerializeSnafu};
use super::http::{HttpErrorData, http_req_with_headers};
use api_structs::auth::LoginPayload;
use cookie::Cookie;
use http::{HeaderMap, HeaderValue, Method, header};
use reqwest;
use serde_json::json;
use snafu::ResultExt;
use std::collections::HashMap;
use std::net::SocketAddr;

pub fn get_set_cookie_from_response_headers(
    headers: &HeaderMap,
) -> HashMap<&str, (&str, &HeaderValue)> {
    let set_cookies = headers.get_all("Set-Cookie");

    let mut set_cookies_map = HashMap::new();

    for value in set_cookies.iter() {
        let name_values = value.to_str().unwrap().split('=').collect::<Vec<_>>();
        let cookie_name = name_values[0];
        let cookie_values = name_values[1].split("; ").collect::<Vec<_>>();
        let cookie_val = cookie_values[0];
        set_cookies_map.insert(cookie_name, (cookie_val, value));
    }
    set_cookies_map
}

pub fn get_set_cookie_name_value_map(headers: &HeaderMap) -> HashMap<String, String> {
    let values = get_set_cookie_from_response_headers(headers);

    let mut cookies = HashMap::new();
    for (name, value) in values {
        let cookie_str = value.1.to_str().unwrap();
        if let Ok(cookie) = Cookie::parse(cookie_str) {
            cookies.insert(cookie.name().to_string(), cookie.value().to_string());
        }
    }
    cookies
}
