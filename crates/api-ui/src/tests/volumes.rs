#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::tests::common::http_req;
use crate::tests::common::{Entity, Op, req, ui_test_op};
use crate::tests::server::run_test_server;
use crate::volumes::models::{
    AwsAccessKeyCredentials, AwsCredentials, FileVolume, S3TablesVolume, S3Volume, Volume,
    VolumeCreatePayload, VolumeCreateResponse, VolumeType, VolumesResponse,
};
use core_metastore::models::VolumeType as MetastoreVolumeType;
use http::Method;
use serde_json;
use serde_json::json;

fn create_s3_volume_ok_payload() -> VolumeCreatePayload {
    VolumeCreatePayload {
        name: "embucket3".to_string(),
        volume: VolumeType::S3(S3Volume {
            region: Some("us-west-1".to_string()),
            bucket: Some("embucket".to_string()),
            endpoint: Some("https://localhost:9000".to_string()),
            credentials: Some(AwsCredentials::AccessKey(AwsAccessKeyCredentials {
                aws_access_key_id: "kPYGGu34jF685erC7gst".to_string(),
                aws_secret_access_key: "Q2ClWJgwIZLcX4IE2zO2GBl8qXz7g4knqwLwUpWL".to_string(),
            })),
        }),
    }
}

fn create_s3_tables_volume_ok_payload() -> VolumeCreatePayload {
    let res = VolumeCreatePayload {
        name: "embucket3".to_string(),
        volume: VolumeType::S3Tables(S3TablesVolume {
            arn: "arn:aws:s3tables:us-east-1:111122223333:bucket/my-embucket".to_string(),
            endpoint: Some("https://localhost:9000".to_string()),
            credentials: AwsCredentials::AccessKey(AwsAccessKeyCredentials {
                aws_access_key_id: "kPYGGu34jF685erC7gst".to_string(),
                aws_secret_access_key: "Q2ClWJgwIZLcX4IE2zO2GBl8qXz7g4knqwLwUpWL".to_string(),
            }),
        }),
    };

    // check bucket can be fetched from arn
    let metastore_volume: MetastoreVolumeType = res.volume.clone().into();
    if let MetastoreVolumeType::S3Tables(ref s3_tables_volume) = metastore_volume {
        assert_eq!("my-embucket", s3_tables_volume.bucket().unwrap());
    }

    res
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_volumes() {
    let addr = run_test_server();
    let client = reqwest::Client::new();

    // memory volume with empty ident create Ok
    let expected = VolumeCreatePayload {
        name: "embucket1".to_string(),
        volume: VolumeType::Memory,
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Volume(expected.clone())).await;
    assert_eq!(200, res.status());
    let VolumeCreateResponse(created) = res.json().await.unwrap();
    assert_eq!(expected.name, created.name);

    // memory volume with empty ident create Ok
    let payload = r#"{"name":"embucket2","type": "file", "path":"/tmp/data"}"#;
    let expected: VolumeCreatePayload = serde_json::from_str(payload).unwrap();
    let res = ui_test_op(addr, Op::Create, None, &Entity::Volume(expected.clone())).await;
    // let res = create_test_volume(addr, &expected).await;
    assert_eq!(200, res.status());
    let VolumeCreateResponse(created) = res.json().await.unwrap();
    assert_eq!(expected.name, created.name);

    let expected = VolumeCreatePayload {
        name: "embucket2".to_string(),
        volume: VolumeType::File(FileVolume {
            path: "/tmp/data".to_string(),
        }),
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Volume(expected.clone())).await;
    // let res = create_test_volume(addr, &expected).await;
    assert_eq!(409, res.status());

    // memory volume with empty ident create Ok
    let expected = create_s3_volume_ok_payload();
    let created_s3_volume = http_req::<Volume>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/volumes"),
        json!(expected).to_string(),
    )
    .await
    .expect("Failed to create s3 volume");
    assert_eq!(expected.name, created_s3_volume.name);

    //Get list volumes
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/volumes",).to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let volumes_response: VolumesResponse = res.json().await.unwrap();
    assert_eq!(3, volumes_response.items.len());
    assert_eq!(
        "embucket3".to_string(),
        volumes_response.items.first().unwrap().name
    );

    //Get list volumes with parameters
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/volumes?limit=2",).to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let volumes_response: VolumesResponse = res.json().await.unwrap();
    assert_eq!(
        vec!["embucket3".to_string(), "embucket2".to_string()],
        volumes_response.items.iter().map(|d| d.name.clone()).collect::<Vec<_>>(),
    );

    //Get list volumes with parameters
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/volumes?offset=2",).to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let volumes_response: VolumesResponse = res.json().await.unwrap();
    assert_eq!(1, volumes_response.items.len());
    assert_eq!(
        "embucket1".to_string(),
        volumes_response.items.first().unwrap().name
    );

    //Create a volume with diffrent name
    let expected = VolumeCreatePayload {
        name: "icebucket1".to_string(),
        volume: VolumeType::Memory,
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Volume(expected.clone())).await;
    assert_eq!(200, res.status());
    let VolumeCreateResponse(created) = res.json().await.unwrap();
    assert_eq!(expected.name, created.name);

    //Get list volumes
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/volumes",).to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let volumes_response: VolumesResponse = res.json().await.unwrap();
    assert_eq!(4, volumes_response.items.len());

    //Get list volumes with parameters
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/volumes?search=embucket",).to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let volumes_response: VolumesResponse = res.json().await.unwrap();
    assert_eq!(3, volumes_response.items.len());

    //Get list volumes with parameters
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/volumes?search=embucket&orderDirection=ASC",).to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let volumes_response: VolumesResponse = res.json().await.unwrap();
    assert_eq!(3, volumes_response.items.len());
    assert_eq!(
        "embucket1".to_string(),
        volumes_response.items.first().unwrap().name
    );

    //Get list volumes with parameters
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/volumes?search=ice",).to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let volumes_response: VolumesResponse = res.json().await.unwrap();
    assert_eq!(1, volumes_response.items.len());
    assert_eq!(
        "icebucket1".to_string(),
        volumes_response.items.first().unwrap().name
    );

    //Delete volume
    let res = req(
        &client,
        Method::DELETE,
        &format!("http://{addr}/ui/volumes/embucket1",).to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());

    //Get list volumes
    let res = req(
        &client,
        Method::GET,
        &format!("http://{addr}/ui/volumes",).to_string(),
        String::new(),
    )
    .await
    .unwrap();
    assert_eq!(http::StatusCode::OK, res.status());
    let volumes_response: VolumesResponse = res.json().await.unwrap();
    assert_eq!(3, volumes_response.items.len());
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_s3_volumes_validation() {
    let addr = run_test_server();
    let client = reqwest::Client::new();

    let mut create_s3_volume_bad_endpoint_payload = create_s3_volume_ok_payload();
    if let VolumeType::S3(ref mut s3_volume) = create_s3_volume_bad_endpoint_payload.volume {
        // set bad endpoint
        s3_volume.endpoint = Some("localhost:9000".to_string());
    }
    let res = http_req::<VolumeCreateResponse>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/volumes"),
        json!(create_s3_volume_bad_endpoint_payload).to_string(),
    )
    .await
    .expect_err("Should fail as of bad endpoint");
    assert_eq!(422, res.status);

    let mut create_s3_volume_bad_cred_payload = create_s3_volume_ok_payload();
    if let VolumeType::S3(ref mut s3_volume) = create_s3_volume_bad_cred_payload.volume {
        // set bad endpoint
        s3_volume.credentials = Some(AwsCredentials::AccessKey(AwsAccessKeyCredentials {
            aws_access_key_id: "bad".to_string(),
            aws_secret_access_key: "bad".to_string(),
        }));
    }
    let res = http_req::<VolumeCreateResponse>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/volumes"),
        json!(create_s3_volume_bad_cred_payload).to_string(),
    )
    .await
    .expect_err("Should fail as of bad creds");
    assert_eq!(422, res.status);

    http_req::<VolumeCreateResponse>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/volumes"),
        json!(create_s3_volume_ok_payload()).to_string(),
    )
    .await
    .expect("Should create s3 volume");
}

#[test]
fn test_serde_roundtrip() {
    // https://github.com/Embucket/embucket/issues/1306
    let payload: VolumeCreatePayload = create_s3_tables_volume_ok_payload();
    let json = serde_json::to_string(&payload).expect("Failed to serialize");
    serde_json::from_str::<VolumeCreatePayload>(&json).expect("Failed to parse");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_s3_tables_volumes_validation() {
    let addr = run_test_server();
    let client = reqwest::Client::new();

    let mut create_s3_tables_volume_bad_endpoint_payload = create_s3_tables_volume_ok_payload();
    if let VolumeType::S3Tables(ref mut s3_tables_volume) =
        create_s3_tables_volume_bad_endpoint_payload.volume
    {
        // set bad endpoint: not http:// or https:// is set
        s3_tables_volume.endpoint = Some("localhost:9000".to_string());
    }
    let res = http_req::<VolumeCreateResponse>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/volumes"),
        json!(create_s3_tables_volume_bad_endpoint_payload).to_string(),
    )
    .await
    .expect_err("Create s3 table volume should fail as of bad endpoint");
    assert_eq!(422, res.status);

    let mut create_s3_tables_volume_bad_arn_payload = create_s3_tables_volume_ok_payload();
    if let VolumeType::S3Tables(ref mut s3_tables_volume) =
        create_s3_tables_volume_bad_arn_payload.volume
    {
        s3_tables_volume.arn = "bad".to_string();
    }
    let res = http_req::<VolumeCreateResponse>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/volumes"),
        json!(create_s3_tables_volume_bad_arn_payload).to_string(),
    )
    .await
    .expect_err("Create s3 table volume should fail as of bad arn");
    assert_eq!(422, res.status);

    // Following is valid, as endpoint is optional
    let mut create_s3_tables_volume_no_endpoint_payload = create_s3_tables_volume_ok_payload();
    if let VolumeType::S3Tables(ref mut s3_tables_volume) =
        create_s3_tables_volume_no_endpoint_payload.volume
    {
        s3_tables_volume.endpoint = None;
    }
    http_req::<VolumeCreateResponse>(
        &client,
        Method::POST,
        &format!("http://{addr}/ui/volumes"),
        json!(create_s3_tables_volume_no_endpoint_payload).to_string(),
    )
    .await
    .expect("Failed to create s3 table volume");
}
