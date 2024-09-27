use std::result::Result;
use axum::{Json, extract::State, extract::Path};
use axum_macros::debug_handler;
use uuid::Uuid;
use crate::schemas::namespaces::{
    NamespaceIdent,
    NamespaceSchema,
};
use catalog::models::Namespace;
use catalog::service::{
    CatalogService,
    Catalog,
};
use catalog::repository::Repository;

use crate::state::AppState;
use crate::error::AppError;


#[debug_handler]
pub async fn create_namespace(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<NamespaceSchema>,
) -> Result<Json<NamespaceSchema>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = CatalogService::new(
        state.catalog_repo.clone(),
        wh,
    );
    let namespace = catalog.create_namespace(
        &payload.namespace,
        payload.properties.unwrap_or_default(),
    ).await?;

    
    Ok(Json(namespace.into()))
}

pub async fn get_namespace(
    State(state): State<AppState>,
    Path((id, namespace_id)): Path<(Uuid, String)>,
) -> Result<Json<NamespaceSchema>, AppError> {
    println!("get_namespace");
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = CatalogService::new(
        state.catalog_repo.clone(),
        wh,
    );
    // TODO: automatically convert from NamespaceIdent in Path<NamespaceIdent> to NamespaceIdent
    let namespace_id = NamespaceIdent::new(namespace_id);
    let namespace = catalog.get_namespace(&namespace_id).await?;
    
    Ok(Json(namespace.into()))
}

pub async fn delete_namespace(
    State(state): State<AppState>,
    Path((id, namespace_id)): Path<(Uuid, String)>,
) -> Result<Json<()>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = CatalogService::new(
        state.catalog_repo.clone(),
        wh,
    );
    let namespace_id = NamespaceIdent::new(namespace_id);
    catalog.drop_namespace(&namespace_id).await?;
    
    Ok(Json(()))
}

pub async fn list_namespaces(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<NamespaceSchema>>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = CatalogService::new(
        state.catalog_repo.clone(),
        wh,
    );
    let parent_id = None;
    let namespaces = catalog.list_namespaces(parent_id).await?;
    
    Ok(Json(namespaces.into_iter().map(|n: NamespaceIdent| NamespaceSchema{
        namespace: n,
        properties: None,
    }).collect()))
}
