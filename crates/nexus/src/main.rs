use catalog::repository::{DatabaseRepositoryDb, TableRepositoryDb};
use catalog::service::CatalogImpl;
use control_plane::repository::{StorageProfileRepositoryDb, WarehouseRepositoryDb};
use control_plane::service::ControlServiceImpl;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use slatedb::config::DbOptions;
use slatedb::db::Db as SlateDb;
use std::sync::Arc;

use utils::Db;

pub mod http {
    pub mod router;
    pub mod control {
        pub mod handlers;
        pub mod router;
        pub mod schemas;
    }
    pub mod catalog {
        pub mod handlers;
        pub mod router;
        pub mod schemas;
    }
}
pub mod error;
pub mod state;

#[tokio::main]
async fn main() {
    let db = {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let options = DbOptions::default();
        let db = Arc::new(Db::new(
            SlateDb::open_with_opts(Path::from("/tmp/test_kv_store"), options, object_store)
                .await
                .unwrap(),
        ));
        db
    };

    // Initialize the repository and concrete service implementation
    let control_svc = {
        let storage_profile_repo = StorageProfileRepositoryDb::new(db.clone());
        let warehouse_repo = WarehouseRepositoryDb::new(db.clone());
        ControlServiceImpl::new(Arc::new(storage_profile_repo), Arc::new(warehouse_repo))
    };

    let catalog_svc = {
        let t_repo = TableRepositoryDb::new(db.clone());
        let db_repo = DatabaseRepositoryDb::new(db.clone());

        CatalogImpl::new(Arc::new(t_repo), Arc::new(db_repo))
    };

    // Create the application state
    let app_state = state::AppState::new(Arc::new(control_svc), Arc::new(catalog_svc));

    // Create the application router and pass the state
    let app = http::router::create_app(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
