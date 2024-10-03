use std::fs;
use std::path::Path;
use utoipa::OpenApi;

use nexus::schemas::tables::TableApi;
use nexus::schemas::namespace::NamespaceApi;
use nexus::schemas::warehouse::WarehouseApi;
use nexus::schemas::storage_profile::StorageProfileApi;


#[derive(OpenApi)]
#[openapi(
    nest(
        (path = "/v1/storage-profile", api = StorageProfileApi, tags = ["storage-profile"]),
        (path = "/v1/warehouse", api = WarehouseApi, tags = ["warehouse"]),
        (path = "/v1/warehouse/{warehouseId}/namespace", api = NamespaceApi, tags = ["database"]),
        (path = "/v1/warehouse/{warehouseId}/namespace/{namespaceId}/table", api = TableApi, tags = ["table"]),
    ),
    tags(
        (name = "storage-profile", description = "Storage profile API"),
        (name = "warehouse", description = "Warehouse API"),
    )
)]
pub struct ApiDoc;

fn main() {
    // TODO: Finish spec generation either during build or at runtime
    CONST SOURCE_SPEC: &str = "rest-catalog-open-api.yaml";
    CONST DEST_SPEC: &str = "openapi.yaml";
    // Define the output path for the generated OpenAPI spec
    let dest_path = Path::new(&out_dir).join("openapi.yaml");

    let openapi_yaml_content =
        fs::read_to_string("rest-catalog-open-api.yaml").expect("Failed to read OpenAPI spec file");

    // Parse the YAML content
    let mut original_spec = serde_yaml::from_str::<openapi::OpenApi>(&openapi_yaml_content)
        .expect("Failed to parse openapi.yaml file");
    // Dropping all paths from the original spec
    original_spec.paths = openapi::Paths::new();

    ApiDoc::openapi().merge_from(original_spec);
}
