import dataclasses
import os
import urllib
import uuid

import pyiceberg.catalog
import pyiceberg.catalog.rest
import pyiceberg.typedef
import pytest
import requests

# ---- Core
MANAGEMENT_URL = os.environ.get("MANAGEMENT_URL", "http://localhost:3000")
CATALOG_URL = os.environ.get("CATALOG_URL", "http://localhost:3000/catalog")
# ---- S3
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "access-key")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "secret-key")
S3_BUCKET = os.environ.get("S3_BUCKET", "mybucket")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "https://s3.us-east-2amazonaws.com")
S3_REGION = os.environ.get("S3_REGION", "us-east-2")
S3_PATH_STYLE_ACCESS = os.environ.get("S3_PATH_STYLE_ACCESS")


@dataclasses.dataclass
class Namespace:
    name: [str]
    properties: dict
    warehouse_id: str


@dataclasses.dataclass
class Server:
    catalog_url: str
    management_url: str

    def create_storage_profile(self, **payload) -> dict:
        storage_profile_url = urllib.parse.urljoin(
            self.management_url, "v1/storage-profile"
        )
        # payload = {k.replace("_", "-"): v for k, v in payload.items()}
        response = requests.post(
            storage_profile_url,
            json=payload,
        )
        assert response.ok, response.text
        return response.json()

    def delete_storage_profile(self, storage_profile: dict) -> None:
        sid = storage_profile.get("id", None)
        assert sid
        storage_profile_url = urllib.parse.urljoin(
            self.management_url, f"v1/storage-profile/{sid}"
        )
        response = requests.delete(
            storage_profile_url,
            json=storage_profile,
        )
        assert response.ok, response.text

    def create_warehouse(
            self,
            **payload,
    ) -> uuid.UUID:
        """Create a warehouse in this server"""
        warehouse_url = urllib.parse.urljoin(self.management_url, "v1/warehouse")
        # payload = {k.replace("_", "-"): v for k, v in payload.items()}
        response = requests.post(
            warehouse_url,
            json=payload,
        )
        assert response.ok, response.text
        return response.json()

    def delete_warehouse(self, warehouse: dict) -> None:
        wid = warehouse.get("id", None)
        assert wid
        warehouse_url = urllib.parse.urljoin(self.management_url, f"v1/warehouse/{wid}")
        response = requests.delete(
            warehouse_url,
            json=warehouse,
        )
        assert response.ok, response.text


@pytest.fixture(scope="session")
def server() -> Server:
    if MANAGEMENT_URL is None:
        pytest.skip("ICEBERG_REST_TEST_MANAGEMENT_URL is not set")
    if CATALOG_URL is None:
        pytest.skip("ICEBERG_REST_TEST_CATALOG_URL is not set")

    return Server(
        catalog_url=CATALOG_URL.rstrip("/") + "/",
        management_url=MANAGEMENT_URL.rstrip("/") + "/",
    )


@pytest.fixture(scope="session")
def storage_profile(server: Server) -> dict:
    data = server.create_storage_profile(
        type="Aws",
        region=S3_REGION,
        bucket=S3_BUCKET,
        credentials={
            "credential_type": "access_key",
            "aws_access_key_id": S3_ACCESS_KEY,
            "aws_secret_access_key": S3_SECRET_KEY,
        },
        sts_role_arn=None,
        endpoint=S3_ENDPOINT,
    )

    yield data
    server.delete_storage_profile(data)


@pytest.fixture(scope="session")
def warehouse(server: Server, storage_profile) -> dict:
    warehouse_name = "test-warehouse"
    prefix = "warehouse-prefix"
    wh = server.create_warehouse(
        name=warehouse_name,
        prefix=prefix,
        storage_profile_id=storage_profile.get("id", None),
    )
    yield wh
    server.delete_warehouse(wh)


@pytest.fixture(scope="session")
def namespace(catalog) -> dict:
    namespace = ("test-namespace",)
    catalog.create_namespace(namespace)
    return Namespace(
        name=namespace,
        properties={},
        warehouse_id=catalog.properties.get("warehouse", None),
    )


@pytest.fixture(scope="session")
def catalog(warehouse):
    return pyiceberg.catalog.rest.RestCatalog(
        name="test-catalog",
        uri=CATALOG_URL.rstrip("/") + "/",
        warehouse=warehouse.get("id", None),
    )
