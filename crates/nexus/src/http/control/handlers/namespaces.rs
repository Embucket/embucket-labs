use crate::http::control::schemas::namespaces::{NamespaceIdent, NamespaceSchema, NamespaceIdentDuplicate};
use axum::Router;
use axum::{extract::Path, extract::State, Json};
use axum_macros::debug_handler;
use catalog::models::Namespace;
use catalog::repository::Repository;
use catalog::service::{Catalog as CatalogExt, CatalogImpl};
use iceberg::Catalog;
use std::result::Result;
use utoipa::OpenApi;
use uuid::Uuid;

use crate::error::AppError;
use crate::state::AppState;

#[derive(OpenApi)]
#[openapi(
    paths(create_namespace, get_namespace, delete_namespace, list_namespaces,),
    components(schemas(NamespaceSchema, NamespaceIdentDuplicate),)
)]
pub struct NamespaceApi;


#[utoipa::path(
    post, 
    operation_id = "createNamespace",
    path = "", 
    params(("warehouseId" = Uuid, description = "Warehouse ID")),
    request_body = NamespaceSchema,
    responses((status = 200, body = NamespaceSchema))
)]
pub async fn create_namespace(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<NamespaceSchema>,
) -> Result<Json<NamespaceSchema>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = CatalogImpl::new(state.catalog_repo.clone(), wh);
    let namespace = catalog
        .create_namespace(&payload.namespace, payload.properties.unwrap_or_default())
        .await?;

    Ok(Json(namespace.into()))
}

#[utoipa::path(
    get, 
    operation_id = "getNamespace",
    path = "/{namespaceId}", 
    params(("warehouseId" = Uuid, description = "Warehouse ID"), ("namespaceId" = String, description = "Namespace ID")),
    responses((status = 200, body = NamespaceSchema)),
)]
pub async fn get_namespace(
    State(state): State<AppState>,
    Path((id, namespace_id)): Path<(Uuid, String)>,
) -> Result<Json<NamespaceSchema>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = CatalogImpl::new(state.catalog_repo, wh);
    // TODO: automatically convert from NamespaceIdent in Path<NamespaceIdent> to NamespaceIdent
    let namespace_id = NamespaceIdent::new(namespace_id);
    let namespace = catalog.get_namespace(&namespace_id).await?;

    Ok(Json(namespace.into()))
}

#[utoipa::path(
    delete, 
    operation_id = "deleteNamespace",
    path = "/{namespaceId}", 
    params(("warehouseId" = Uuid, description = "Warehouse ID"), ("namespaceId" = String, description = "Namespace ID")),
    responses((status = 200))
)]
pub async fn delete_namespace(
    State(state): State<AppState>,
    Path((id, namespace_id)): Path<(Uuid, String)>,
) -> Result<Json<()>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = CatalogImpl::new(state.catalog_repo, wh);
    let namespace_id = NamespaceIdent::new(namespace_id);
    catalog.drop_namespace(&namespace_id).await?;

    Ok(Json(()))
}

#[utoipa::path(
    get, 
    operation_id = "listNamespaces",
    path = "", 
    params(("warehouseId" = Uuid, description = "Warehouse ID")),
    responses((status = 200, body = Vec<NamespaceSchema>))
)]
pub async fn list_namespaces(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<NamespaceSchema>>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = CatalogImpl::new(state.catalog_repo, wh);
    let parent_id = None;
    let namespaces = catalog.list_namespaces(parent_id).await?;

    Ok(Json(
        namespaces
            .into_iter()
            .map(|n: NamespaceIdent| NamespaceSchema {
                namespace: n,
                properties: None,
            })
            .collect(),
    ))
}
