pub mod error;
// pub mod old_handlers;
// pub mod old_models;
pub mod router;

pub mod dashboard;
pub mod databases;
pub mod navigation_trees;
pub mod queries;
pub mod schemas;
pub mod tables;
#[cfg(test)]
pub mod tests;
pub mod volumes;
pub mod worksheets;

//Default limit for pagination
#[allow(clippy::unnecessary_wraps)]
const fn default_limit() -> Option<u16> {
    Some(250)
}
