use crate::error::AppError;
use crate::http::catalog::schemas;
use crate::http::utils::{get_default_properties, update_properties_timestamps};
use crate::state::AppState;
use axum::{extract::Path, extract::Query, extract::State, Json};
use catalog::models::{DatabaseIdent, NamespaceIdent, TableCommit, TableIdent, WarehouseIdent};
use std::result::Result;
use uuid::Uuid;

use control_plane::models::StorageProfile;

pub async fn create_namespace(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<schemas::Namespace>,
) -> Result<Json<schemas::Namespace>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = state.catalog_svc;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(wh.id),
        namespace: payload.namespace.clone(),
    };
    let mut properties = payload.properties.unwrap_or_default();
    update_properties_timestamps(&mut properties);

    let res = catalog.create_namespace(&ident, properties).await?;

    Ok(Json(res.into()))
}

pub async fn get_namespace(
    State(state): State<AppState>,
    Path((id, namespace_id)): Path<(Uuid, String)>,
) -> Result<Json<schemas::Namespace>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = state.catalog_svc;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(wh.id),
        namespace: NamespaceIdent::new(namespace_id),
    };
    let namespace = catalog.get_namespace(&ident).await?;

    Ok(Json(namespace.into()))
}

pub async fn delete_namespace(
    State(state): State<AppState>,
    Path((id, namespace_id)): Path<(Uuid, String)>,
) -> Result<Json<()>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = state.catalog_svc;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(wh.id),
        namespace: NamespaceIdent::new(namespace_id),
    };
    catalog.drop_namespace(&ident).await?;

    Ok(Json(()))
}

pub async fn list_namespaces(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<schemas::NamespaceListResponse>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = state.catalog_svc;
    let ident = WarehouseIdent::new(wh.id);
    // TODO: Extract parent_id from request
    let parent_id = None;
    let databases = catalog.list_namespaces(&ident, parent_id).await?;

    // TODO: Implement From/Into
    Ok(Json(schemas::NamespaceListResponse {
        namespaces: databases.into_iter().map(Into::into).collect(),
    }))
}

pub async fn create_table(
    State(state): State<AppState>,
    Path((id, namespace_id)): Path<(Uuid, String)>,
    Json(payload): Json<schemas::TableCreateRequest>,
) -> Result<Json<schemas::TableResult>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let sp = state.control_svc.get_profile(wh.storage_profile_id).await?;
    let catalog = state.catalog_svc;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(wh.id),
        namespace: NamespaceIdent::new(namespace_id),
    };
    let table = catalog
        .create_table(
            &ident,
            &sp,
            &wh,
            payload.into(),
            Option::from(get_default_properties()),
        )
        .await?;

    Ok(Json(table.into()))
}

pub async fn commit_table(
    State(state): State<AppState>,
    Path((id, namespace_id, table_id)): Path<(Uuid, String, String)>,
    Json(payload): Json<schemas::TableCommitRequest>,
) -> Result<Json<schemas::TableResult>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let sp = state.control_svc.get_profile(wh.storage_profile_id).await?;
    let catalog = state.catalog_svc;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(wh.id),
        namespace: NamespaceIdent::new(namespace_id),
    };
    // FIXME: Iceberg REST has table ident in the body request
    let ident = TableIdent {
        database: ident,
        table: table_id,
    };
    let commit = TableCommit {
        ident,
        requirements: payload.requirements,
        updates: payload.updates,
    };
    let table = catalog.update_table(&sp, &wh, commit).await?;

    Ok(Json(table.into()))
}

pub async fn get_table(
    State(state): State<AppState>,
    Path((id, namespace_id, table_id)): Path<(Uuid, String, String)>,
) -> Result<Json<schemas::TableResult>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = state.catalog_svc;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(wh.id),
        namespace: NamespaceIdent::new(namespace_id),
    };
    let ident = TableIdent {
        database: ident,
        table: table_id,
    };
    let table = catalog.load_table(&ident).await?;

    Ok(Json(table.into()))
}

pub async fn delete_table(
    State(state): State<AppState>,
    Path((id, namespace_id, table_id)): Path<(Uuid, String, String)>,
) -> Result<Json<()>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = state.catalog_svc;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(wh.id),
        namespace: NamespaceIdent::new(namespace_id),
    };
    let ident = TableIdent {
        database: ident,
        table: table_id,
    };
    catalog.drop_table(&ident).await?;

    Ok(Json(()))
}

pub async fn list_tables(
    State(state): State<AppState>,
    Path((id, namespace_id)): Path<(Uuid, String)>,
) -> Result<Json<schemas::TableListResponse>, AppError> {
    let wh = state.control_svc.get_warehouse(id).await?;
    let catalog = state.catalog_svc;
    let ident = DatabaseIdent {
        warehouse: WarehouseIdent::new(wh.id),
        namespace: NamespaceIdent::new(namespace_id),
    };
    let tables = catalog.list_tables(&ident).await?;

    Ok(Json(schemas::TableListResponse {
        identifiers: tables.into_iter().map(Into::into).collect(),
    }))
}

pub async fn report_metrics(
    State(state): State<AppState>,
    Path((id, namespace_id, table_id)): Path<(Uuid, String, String)>,
    Json(payload): Json<()>,
) -> Result<(), AppError> {
    println!("add_table_metrics: {:?}", payload);
    Ok(())
}

pub async fn get_config(
    State(state): State<AppState>,
    Query(params): Query<schemas::GetConfigQueryParams>,
) -> Result<Json<schemas::Config>, AppError> {
    let wh_id = params.warehouse;
    let mut ident: Option<WarehouseIdent> = None;
    let mut sp: Option<StorageProfile> = None;
    if let Some(value) = wh_id {
        let wh = state.control_svc.get_warehouse(value).await?;
        sp = Some(state.control_svc.get_profile(wh.storage_profile_id).await?);
        ident = Some(WarehouseIdent::new(wh.id));
    }

    let config = state.catalog_svc.get_config(ident, sp).await?;

    Ok(Json(config.into()))
}

// only one endpoint is defined for the catalog implementation to work
// we don't actually have functionality for views yet
pub async fn list_views(
    State(state): State<AppState>,
    Path((id, namespace_id)): Path<(Uuid, String)>,
) -> Result<Json<schemas::TableListResponse>, AppError> {
    Ok(Json(schemas::TableListResponse {
        identifiers: vec![],
    }))
}
