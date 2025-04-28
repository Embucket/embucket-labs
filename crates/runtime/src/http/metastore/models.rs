use embucket_metastore::models::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HTTPRwObject<T: ToSchema + Eq + Clone>(pub RwObject<T>);

impl<T: ToSchema + Eq + Clone> From<RwObject<T>> for HTTPRwObject<T> {
    fn from(rw_object: RwObject<T>) -> Self {
        Self(rw_object)
    }
}

impl<T: ToSchema + Eq + Clone> From<HTTPRwObject<T>> for RwObject<T> {
    fn from(http_rw_object: HTTPRwObject<T>) -> Self {
        http_rw_object.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HTTPRwObjectVec<T: ToSchema + Eq + Clone>(pub Vec<HTTPRwObject<T>>);

impl<T: ToSchema + Eq + Clone> From<Vec<RwObject<T>>> for HTTPRwObjectVec<T> {
    fn from(rw_objects: Vec<RwObject<T>>) -> Self {
        Self(rw_objects.into_iter().map(HTTPRwObject::from).collect())
    }
}

impl<T: ToSchema + Eq + Clone> From<HTTPRwObjectVec<T>> for Vec<RwObject<T>> {
    fn from(http_rw_objects: HTTPRwObjectVec<T>) -> Self {
        http_rw_objects.0.into_iter().map(RwObject::from).collect()
    }
}
