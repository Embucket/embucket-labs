pub mod auth;
pub mod config;
pub mod dashboard;
pub mod databases;
pub mod error;
pub mod layers;
pub mod navigation_trees;
pub mod queries;
pub mod router;
pub mod schemas;
pub mod state;
pub mod tables;
#[cfg(test)]
pub mod tests;
pub mod volumes;
pub mod web_assets;
pub mod worksheets;

//Default limit for pagination
#[allow(clippy::unnecessary_wraps)]
const fn default_limit() -> Option<u16> {
    Some(250)
}
