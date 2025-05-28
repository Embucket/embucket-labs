//! Helper functions for handling HTTP requests and responses.
//!
//! This module provides utility functions for working with HTTP headers and cookies
//! in the context of the Embucket API client.

use cookie::Cookie;
use http::{HeaderMap, HeaderValue};
use std::collections::HashMap;

/// Extracts all Set-Cookie headers from an HTTP response and returns them as a map.
///
/// The returned map uses cookie names as keys and tuples of (`cookie_value`, `original_header_value`)
/// as values.
///
/// # Arguments
/// * `headers` - The HTTP headers from the response
///
/// # Returns
/// A map where keys are cookie names and values are tuples of (`cookie_value`, `header_value`)
#[allow(clippy::explicit_iter_loop)]
#[must_use]
pub fn get_set_cookie_from_response_headers(
    headers: &HeaderMap,
) -> HashMap<&str, (&str, &HeaderValue)> {
    let set_cookies = headers.get_all("Set-Cookie");

    let mut set_cookies_map = HashMap::new();

    for value in set_cookies.iter() {
        if let Ok(value_str) = value.to_str() {
            let name_values = value_str.split('=').collect::<Vec<_>>();
            let cookie_name = name_values[0];
            let cookie_values = name_values[1].split("; ").collect::<Vec<_>>();
            let cookie_val = cookie_values[0];
            set_cookies_map.insert(cookie_name, (cookie_val, value));
        }
    }
    set_cookies_map
}

/// Extracts all cookies from Set-Cookie headers and returns them as a name-value map.
///
/// This function parses the Set-Cookie headers and extracts cookie names and values,
/// ignoring other cookie attributes like path, domain, etc.
///
/// # Arguments
/// * `headers` - The HTTP headers from the response
///
/// # Returns
/// A map where keys are cookie names and values are cookie values
#[must_use]
pub fn get_set_cookie_name_value_map(headers: &HeaderMap) -> HashMap<String, String> {
    let values = get_set_cookie_from_response_headers(headers);

    let mut cookies = HashMap::new();
    for (_name, value) in values {
        if let Ok(cookie_str) = value.1.to_str() {
            if let Ok(cookie) = Cookie::parse(cookie_str) {
                cookies.insert(cookie.name().to_string(), cookie.value().to_string());
            }
        }
    }
    cookies
}
