// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::http::tests::common::{create_server, ui_test_op, Entity, Op};
use icebucket_metastore::IceBucketVolume;
use icebucket_metastore::{
    AwsAccessKeyCredentials, AwsCredentials, IceBucketFileVolume, IceBucketS3Volume,
    IceBucketVolumeType,
};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_volumes_memory() {
    let addr = create_server().await;

    // memory volume with empty ident create Ok
    let expected = IceBucketVolume {
        ident: String::new(),
        volume: IceBucketVolumeType::Memory,
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Volume(expected.clone())).await;
    assert_eq!(200, res.status());
    let created = res.json::<IceBucketVolume>().await.unwrap();
    assert_eq!(expected, created);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_volumes_file() {
    let addr = create_server().await;

    // memory volume with empty ident create Ok
    let expected = IceBucketVolume {
        ident: "".to_string(),
        volume: IceBucketVolumeType::File(IceBucketFileVolume {
            path: "/tmp/data".to_string(),
        }),
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Volume(expected.clone())).await;
    // let res = create_test_volume(addr, &expected).await;
    assert_eq!(200, res.status());
    let created = res.json::<IceBucketVolume>().await.unwrap();
    assert_eq!(expected, created);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_ui_volumes_s3() {
    let addr = create_server().await;

    // memory volume with empty ident create Ok
    let expected = IceBucketVolume {
        ident: "".to_string(),
        volume: IceBucketVolumeType::S3(IceBucketS3Volume {
            region: Some("us-west-1".to_string()),
            bucket: Some("icebucket".to_string()),
            endpoint: Some("http://localhost:9000".to_string()),
            skip_signature: None,
            metadata_endpoint: None,
            credentials: Some(AwsCredentials::AccessKey(AwsAccessKeyCredentials {
                aws_access_key_id: "********".to_string(),
                aws_secret_access_key: "********".to_string(),
            })),
        }),
    };
    let res = ui_test_op(addr, Op::Create, None, &Entity::Volume(expected.clone())).await;
    // let res = create_test_volume(addr, &expected).await;
    assert_eq!(200, res.status());
    let created = res.json::<IceBucketVolume>().await.unwrap();
    assert_eq!(expected, created);
}
