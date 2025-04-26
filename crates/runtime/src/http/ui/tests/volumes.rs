#![allow(clippy::unwrap_used, clippy::expect_used)]
use crate::http::ui::tests::common::{ui_test_op, Entity, Op};
use crate::http::ui::volumes::models::{Volume, VolumeCreatePayload, VolumeCreateResponse};
use crate::tests::run_test_server;
use embucket_metastore::Volume as MetastoreVolume;
use embucket_metastore::{
    AwsAccessKeyCredentials, AwsCredentials, FileVolume as MetastoreFileVolume,
    S3Volume as MetastoreS3Volume, VolumeType as MetastoreVolumeType,
};
use serde_json;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_volumes_memory() {
    let addr = run_test_server().await;

    // memory volume with empty ident create Ok
    let expected = VolumeCreatePayload {
        data: Volume::from(MetastoreVolume {
            ident: String::new(),
            volume: MetastoreVolumeType::Memory,
        }),
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Volume(expected.clone())).await;
    assert_eq!(200, res.status());
    let created = res.json::<VolumeCreateResponse>().await.unwrap();
    assert_eq!(expected.data, created.data);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_volumes_file() {
    let addr = run_test_server().await;

    // memory volume with empty ident create Ok
    let payload = r#"{"name":"","type": "file", "path":"/tmp/data"}"#;
    let expected: VolumeCreatePayload = serde_json::from_str(payload).unwrap();
    let res = ui_test_op(addr, Op::Create, None, &Entity::Volume(expected.clone())).await;
    // let res = create_test_volume(addr, &expected).await;
    assert_eq!(200, res.status());
    let created = res.json::<VolumeCreateResponse>().await.unwrap();
    assert_eq!(expected.data, created.data);

    let expected = VolumeCreatePayload {
        data: Volume::from(MetastoreVolume {
            ident: String::new(),
            volume: MetastoreVolumeType::File(MetastoreFileVolume {
                path: "/tmp/data".to_string(),
            }),
        }),
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Volume(expected.clone())).await;
    // let res = create_test_volume(addr, &expected).await;
    assert_eq!(409, res.status());
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_volumes_s3() {
    let addr = run_test_server().await;

    // memory volume with empty ident create Ok
    let expected = VolumeCreatePayload {
        data: Volume::from(MetastoreVolume {
            ident: String::new(),
            volume: MetastoreVolumeType::S3(MetastoreS3Volume {
                region: Some("us-west-1".to_string()),
                bucket: Some("embucket".to_string()),
                endpoint: Some("http://localhost:9000".to_string()),
                skip_signature: None,
                metadata_endpoint: None,
                credentials: Some(AwsCredentials::AccessKey(AwsAccessKeyCredentials {
                    aws_access_key_id: "********".to_string(),
                    aws_secret_access_key: "********".to_string(),
                })),
            }),
        }),
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Volume(expected.clone())).await;
    // let res = create_test_volume(addr, &expected).await;
    assert_eq!(200, res.status());
    let created = res.json::<VolumeCreateResponse>().await.unwrap();
    assert_eq!(expected.data, created.data);
}
