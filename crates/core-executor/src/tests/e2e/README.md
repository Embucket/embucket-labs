## E2E tests

`cargo e2e` command runs all e2e tests.

### Prerequisites

Should set envs, please check at the repository root `.env_e2e` file that contains the list ov vars needed to be set.

Some of tests may require toxiproxy, some allow to use MinIO, and some require pure AWS for s3, s3tables buckets.

Use following to run toxiproxy and MinIO docker containers:
```
docker run --rm --net=host --name toxiproxy -it ghcr.io/shopify/toxiproxy

docker run -d --rm --name minio -p 9001:9001 -p 9000:9000 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server --console-address :9001 ${PWD}/minio-data
```