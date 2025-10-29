#![allow(clippy::result_large_err)]
#![allow(clippy::large_enum_variant)]
use super::e2e_s3tables_aws::s3tables_client;
use crate::models::QueryContext;
use crate::service::{CoreExecutionService, ExecutionService};
use crate::utils::Config;
use aws_sdk_s3tables;
use chrono::Utc;
use core_history::SlateDBHistoryStore;
use core_metastore::Metastore;
use core_metastore::RwObject;
use core_metastore::SlateDBMetastore;
use core_metastore::Volume as MetastoreVolume;
use core_metastore::error::UtilSlateDBSnafu;
use core_metastore::models::volumes::AwsAccessKeyCredentials;
use core_metastore::models::volumes::AwsCredentials;
use core_metastore::{FileVolume, S3TablesVolume, S3Volume, VolumeType};
use core_utils::Db;
use error_stack::ErrorChainExt;
use futures::future::join_all;
use object_store::ObjectStore;
use object_store::{
    aws::AmazonS3Builder, aws::AmazonS3ConfigKey, aws::S3ConditionalPut, local::LocalFileSystem,
};
use slatedb::DbBuilder;
use snafu::ResultExt;
use snafu::{Location, Snafu};
use std::collections::HashMap;
use std::env::{self, VarError};
use std::fmt;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

// Set following envs, or add to .env

pub const MINIO_OBJECT_STORE_PREFIX: &str = "MINIO_OBJECT_STORE_";
// # Env vars for s3 object store on minio
// MINIO_OBJECT_STORE_AWS_ACCESS_KEY_ID=
// MINIO_OBJECT_STORE_AWS_SECRET_ACCESS_KEY=
// MINIO_OBJECT_STORE_AWS_REGION=us-east-1
// MINIO_OBJECT_STORE_AWS_BUCKET=tables-data
// MINIO_OBJECT_STORE_AWS_ENDPOINT=http://localhost:9000
// MINIO_OBJECT_STORE_AWS_ALLOW_HTTP=true

pub const AWS_OBJECT_STORE_PREFIX: &str = "AWS_OBJECT_STORE_";
// # Env vars for s3 object store on AWS
// AWS_OBJECT_STORE_AWS_ACCESS_KEY_ID=
// AWS_OBJECT_STORE_AWS_SECRET_ACCESS_KEY=
// AWS_OBJECT_STORE_AWS_REGION=us-east-1
// AWS_OBJECT_STORE_AWS_BUCKET=tables-data

pub const E2E_S3VOLUME_PREFIX: &str = "E2E_S3VOLUME_";
// Env vars for S3Volume on minio / AWS (change or remove endpoint):
// E2E_S3VOLUME_AWS_ACCESS_KEY_ID=
// E2E_S3VOLUME_AWS_SECRET_ACCESS_KEY=
// E2E_S3VOLUME_AWS_REGION=us-east-1
// E2E_S3VOLUME_AWS_BUCKET=e2e-store
// E2E_S3VOLUME_AWS_ENDPOINT=http://localhost:9000

pub const E2E_S3TABLESVOLUME_PREFIX: &str = "E2E_S3TABLESVOLUME_";
// Env vars for S3TablesVolume:
// E2E_S3TABLESVOLUME_AWS_ACCESS_KEY_ID=
// E2E_S3TABLESVOLUME_AWS_SECRET_ACCESS_KEY=
// E2E_S3TABLESVOLUME_AWS_ARN=arn:aws:s3tables:us-east-1:111122223333:bucket/my-table-bucket

pub const TEST_SESSION_ID1: &str = "test_session_id1";
pub const TEST_SESSION_ID2: &str = "test_session_id2";
pub const TEST_SESSION_ID3: &str = "test_session_id3";

#[derive(Clone)]
pub struct VolumeConfig {
    pub prefix: Option<&'static str>,
    pub volume_type: TestVolumeType,
    pub volume: &'static str,
    pub database: &'static str,
    pub schema: &'static str,
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub enum TestVolumeType {
    Memory,
    File,
    S3,
    S3Tables,
}

pub const TEST_VOLUME_MEMORY: VolumeConfig = VolumeConfig {
    prefix: None,
    volume_type: TestVolumeType::Memory,
    volume: "volume_memory",
    database: "database_in_memory",
    schema: "public",
};
pub const TEST_VOLUME_FILE: VolumeConfig = VolumeConfig {
    prefix: None,
    volume_type: TestVolumeType::File,
    volume: "volume_file",
    database: "database_in_file",
    schema: "public",
};
pub const TEST_VOLUME_S3: VolumeConfig = VolumeConfig {
    prefix: Some(E2E_S3VOLUME_PREFIX),
    volume_type: TestVolumeType::S3,
    volume: "volume_s3",
    database: "database_in_s3",
    schema: "public",
};
pub const TEST_VOLUME_S3TABLES: VolumeConfig = VolumeConfig {
    prefix: Some(E2E_S3TABLESVOLUME_PREFIX),
    volume_type: TestVolumeType::S3Tables,
    volume: "volume_s3tables",
    database: "database_in_s3tables",
    schema: "public",
};

pub const TEST_DATABASE_NAME: &str = "embucket";

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    TestSlatedb {
        source: slatedb::Error,
        object_store: Arc<dyn ObjectStore>,
        #[snafu(implicit)]
        location: Location,
    },
    TestObjectStore {
        source: object_store::Error,
        #[snafu(implicit)]
        location: Location,
    },
    TestExecution {
        query: String,
        source: crate::Error,
        #[snafu(implicit)]
        location: Location,
    },
    TestS3VolumeConfig {
        source: VarError,
        #[snafu(implicit)]
        location: Location,
    },
    TestS3TablesVolumeConfig {
        source: VarError,
        #[snafu(implicit)]
        location: Location,
    },
    TestMetastore {
        source: core_metastore::Error,
        #[snafu(implicit)]
        location: Location,
    },
    TestHistoryStore {
        source: core_history::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Error corrupting S3 volume: No Aws access key credentials found"))]
    TestCreateS3VolumeWithBadCreds {
        #[snafu(implicit)]
        location: Location,
    },
    TestAwsSdk {
        source: aws_sdk_s3tables::Error,
        #[snafu(implicit)]
        location: Location,
    },
    TestBadVolumeType {
        volume_type: String,
        #[snafu(implicit)]
        location: Location,
    },
    TestToxiProxy {
        source: reqwest::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

#[must_use]
pub fn test_suffix() -> String {
    Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_millis())
        .to_string()
}

pub fn copy_env_to_new_prefix(env_prefix: &str, new_env_prefix: &str, skip_envs: &[&str]) {
    unsafe {
        if !skip_envs.contains(&"AWS_ACCESS_KEY_ID") {
            std::env::set_var(
                format!("{new_env_prefix}AWS_ACCESS_KEY_ID"),
                std::env::var(format!("{env_prefix}AWS_ACCESS_KEY_ID")).unwrap_or_default(),
            );
        }
        if !skip_envs.contains(&"AWS_SECRET_ACCESS_KEY") {
            std::env::set_var(
                format!("{new_env_prefix}AWS_SECRET_ACCESS_KEY"),
                std::env::var(format!("{env_prefix}AWS_SECRET_ACCESS_KEY")).unwrap_or_default(),
            );
        }
        if !skip_envs.contains(&"AWS_REGION") {
            std::env::set_var(
                format!("{new_env_prefix}AWS_REGION"),
                std::env::var(format!("{env_prefix}AWS_REGION")).unwrap_or_default(),
            );
        }
        if !skip_envs.contains(&"AWS_BUCKET") {
            std::env::set_var(
                format!("{new_env_prefix}AWS_BUCKET"),
                std::env::var(format!("{env_prefix}AWS_BUCKET")).unwrap_or_default(),
            );
        }
        if !skip_envs.contains(&"AWS_ENDPOINT") {
            std::env::set_var(
                format!("{new_env_prefix}AWS_ENDPOINT"),
                std::env::var(format!("{env_prefix}AWS_ENDPOINT")).unwrap_or_default(),
            );
        }
        if !skip_envs.contains(&"AWS_ALLOW_HTTP") {
            std::env::set_var(
                format!("{new_env_prefix}AWS_ALLOW_HTTP"),
                std::env::var(format!("{env_prefix}AWS_ALLOW_HTTP")).unwrap_or_default(),
            );
        }
        std::env::set_var(
            format!("{new_env_prefix}AWS_ALLOW_HTTP"),
            std::env::var(format!("{env_prefix}AWS_ALLOW_HTTP")).unwrap_or_default(),
        );
    }
}

pub fn s3_volume(env_prefix: &str) -> Result<S3Volume, Error> {
    let region =
        std::env::var(format!("{env_prefix}AWS_REGION")).context(TestS3VolumeConfigSnafu)?;
    let access_key =
        std::env::var(format!("{env_prefix}AWS_ACCESS_KEY_ID")).context(TestS3VolumeConfigSnafu)?;
    let secret_key = std::env::var(format!("{env_prefix}AWS_SECRET_ACCESS_KEY"))
        .context(TestS3VolumeConfigSnafu)?;
    // endpoint is optional
    let endpoint =
        std::env::var(format!("{env_prefix}AWS_ENDPOINT")).context(TestS3VolumeConfigSnafu);
    let bucket =
        std::env::var(format!("{env_prefix}AWS_BUCKET")).context(TestS3VolumeConfigSnafu)?;

    Ok(S3Volume {
        region: Some(region),
        bucket: Some(bucket),
        endpoint: endpoint.ok(),
        credentials: Some(AwsCredentials::AccessKey(AwsAccessKeyCredentials {
            aws_access_key_id: access_key,
            aws_secret_access_key: secret_key,
        })),
    })
}

pub fn s3_tables_volume(
    _schema_namespace: &str,
    env_prefix: &str,
) -> Result<S3TablesVolume, Error> {
    let access_key = std::env::var(format!("{env_prefix}AWS_ACCESS_KEY_ID"))
        .context(TestS3TablesVolumeConfigSnafu)?;
    let secret_key = std::env::var(format!("{env_prefix}AWS_SECRET_ACCESS_KEY"))
        .context(TestS3TablesVolumeConfigSnafu)?;
    let arn =
        std::env::var(format!("{env_prefix}AWS_ARN")).context(TestS3TablesVolumeConfigSnafu)?;
    let endpoint: Option<String> = std::env::var(format!("{env_prefix}AWS_ENDPOINT"))
        .map(Some)
        .unwrap_or(None);

    Ok(S3TablesVolume {
        endpoint,
        credentials: AwsCredentials::AccessKey(AwsAccessKeyCredentials {
            aws_access_key_id: access_key,
            aws_secret_access_key: secret_key,
        }),
        arn,
    })
}

pub async fn create_s3tables_client(env_prefix: &str) -> Result<aws_sdk_s3tables::Client, Error> {
    // use the same credentials as for s3 tables volume
    let s3_tables_volume = s3_tables_volume("test", env_prefix)?;
    if let AwsCredentials::AccessKey(ref access_key) = s3_tables_volume.credentials {
        return Ok(s3tables_client(
            access_key.aws_access_key_id.clone(),
            access_key.aws_secret_access_key.clone(),
            s3_tables_volume.region(),
            s3_tables_volume.account_id(),
        )
        .await);
    }
    panic!("Unsupported credentials type AwsCredentials::Token");
}

pub type TestPlan = Vec<ParallelTest>;

pub struct ParallelTest(pub Vec<TestQuery>);

pub trait TestQueryCallback: Sync + Send {
    fn err_callback(&self, err: &crate::Error);
}

pub struct TestQuery {
    pub sqls: Vec<&'static str>,
    pub executor: Arc<ExecutorWithObjectStore>,
    pub session_id: &'static str,
    pub expected_res: bool,
    pub err_callback: Option<Box<dyn TestQueryCallback>>,
}

pub struct S3TableStore {
    pub s3_builder: AmazonS3Builder,
}

#[derive(Debug, Clone)]
pub struct S3ObjectStore {
    pub s3_builder: AmazonS3Builder,
}
impl S3ObjectStore {
    #[allow(clippy::or_fun_call)]
    pub fn from_env(env_prefix: &str) -> Result<Self, Error> {
        let region =
            std::env::var(format!("{env_prefix}AWS_REGION")).context(TestS3VolumeConfigSnafu)?;
        let access_key = std::env::var(format!("{env_prefix}AWS_ACCESS_KEY_ID"))
            .context(TestS3VolumeConfigSnafu)?;
        let secret_key = std::env::var(format!("{env_prefix}AWS_SECRET_ACCESS_KEY"))
            .context(TestS3VolumeConfigSnafu)?;
        let endpoint =
            std::env::var(format!("{env_prefix}AWS_ENDPOINT")).context(TestS3VolumeConfigSnafu);
        let allow_http =
            std::env::var(format!("{env_prefix}AWS_ALLOW_HTTP")).context(TestS3VolumeConfigSnafu);
        let bucket =
            std::env::var(format!("{env_prefix}AWS_BUCKET")).context(TestS3VolumeConfigSnafu)?;

        eprintln!("Create s3_object_store: {region}, {bucket}, {endpoint:?}");

        let s3_builder = if endpoint.is_ok() {
            AmazonS3Builder::new()
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key)
                .with_region(region)
                .with_bucket_name(bucket)
                .with_allow_http(allow_http.ok().unwrap_or("false".to_string()) == "true")
                .with_conditional_put(S3ConditionalPut::ETagMatch)
                // don't know how to apply optional endpoint with the builder
                .with_endpoint(endpoint?)
        } else {
            AmazonS3Builder::new()
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key)
                .with_region(region)
                .with_bucket_name(bucket)
                .with_allow_http(allow_http.ok().unwrap_or("false".to_string()) == "true")
                .with_conditional_put(S3ConditionalPut::ETagMatch)
        };

        Ok(Self { s3_builder })
    }
}

pub struct ExecutorWithObjectStore {
    pub executor: CoreExecutionService,
    pub metastore: Arc<dyn Metastore>,
    pub db: Arc<Db>,
    pub object_store_type: ObjectStoreType,
    pub alias: String,
    pub used_volumes: HashMap<TestVolumeType, VolumeConfig>,
}

impl ExecutorWithObjectStore {
    #[must_use]
    pub fn with_alias(mut self, alias: String) -> Self {
        self.alias = alias;
        self
    }

    pub async fn create_sessions(&self) -> Result<(), Error> {
        self.executor
            .create_session(TEST_SESSION_ID1)
            .await
            .context(TestExecutionSnafu {
                query: "create session TEST_SESSION_ID1",
            })?;

        self.executor
            .create_session(TEST_SESSION_ID2)
            .await
            .context(TestExecutionSnafu {
                query: "create session TEST_SESSION_ID2",
            })?;

        self.executor
            .create_session(TEST_SESSION_ID3)
            .await
            .context(TestExecutionSnafu {
                query: "create session TEST_SESSION_ID3",
            })?;
        Ok(())
    }

    // Update volume saved in metastore, can't use metastore trait as it checks existance before write
    // Therefore define our own version of metastore volume saver
    // Saves corrupted aws credentials for s3 volume
    pub(crate) async fn create_s3_volume_with_bad_creds(
        &self,
        volume: Option<VolumeConfig>,
    ) -> Result<(), Error> {
        let volume_name = volume.unwrap_or(TEST_VOLUME_S3).volume;
        let volume_name = volume_name.to_string();
        let db_key = format!("vol/{volume_name}");
        let volume = self
            .db
            .get::<RwObject<MetastoreVolume>>(&db_key)
            .await
            .context(UtilSlateDBSnafu)
            .context(TestMetastoreSnafu)?;
        if let Some(volume) = volume {
            let volume = volume.data;
            // set_bad_aws_credentials_for_bucket, by reversing creds
            if let VolumeType::S3(s3_volume) = volume.volume {
                if let Some(AwsCredentials::AccessKey(access_key)) = s3_volume.credentials {
                    // assign reversed string
                    let aws_credentials = AwsCredentials::AccessKey(AwsAccessKeyCredentials {
                        aws_access_key_id: access_key.aws_access_key_id.chars().rev().collect(),
                        aws_secret_access_key: access_key
                            .aws_secret_access_key
                            .chars()
                            .rev()
                            .collect(),
                    });
                    // wrap as a fresh RwObject, this sets new updated at
                    let rwobject = RwObject::new(MetastoreVolume::new(
                        volume_name.clone(),
                        VolumeType::S3(S3Volume {
                            region: s3_volume.region,
                            bucket: s3_volume.bucket,
                            endpoint: s3_volume.endpoint,
                            credentials: Some(aws_credentials),
                        }),
                    ));
                    eprintln!("Intentionally corrupting volume: {:#?}", rwobject.data);
                    // Use db.put to update volume in metastore
                    self.db
                        .put(&db_key, &rwobject)
                        .await
                        .context(UtilSlateDBSnafu)
                        .context(TestMetastoreSnafu)?;
                    // Probably update_volume could be used instead of db.put,
                    // so use update_volume to update just cached object_store
                    self.metastore
                        .update_volume(&volume_name, rwobject.data)
                        .await
                        .context(TestMetastoreSnafu)?;
                    // Directly check if ObjectStore can't access data using bad credentials
                    let object_store = self
                        .metastore
                        .volume_object_store(&volume_name)
                        .await
                        .context(TestMetastoreSnafu)?;
                    if let Some(object_store) = object_store {
                        let obj_store_res = object_store
                            .get(&object_store::path::Path::from("/"))
                            .await
                            .context(TestObjectStoreSnafu);
                        // fail if object_store read succesfully
                        assert!(obj_store_res.is_err());
                    }
                } else {
                    return Err(TestCreateS3VolumeWithBadCredsSnafu.build());
                }
            }
        }
        Ok(())
    }
}

#[allow(clippy::too_many_lines)]
pub async fn create_volumes(
    metastore: Arc<dyn Metastore>,
    object_store_type: &ObjectStoreType,
    override_volumes: Vec<VolumeConfig>,
) -> Result<HashMap<TestVolumeType, VolumeConfig>, Error> {
    let suffix = object_store_type.suffix();

    let used_volumes: HashMap<TestVolumeType, VolumeConfig> = HashMap::from([
        (
            TestVolumeType::Memory,
            override_volumes
                .iter()
                .find(|volume| volume.volume_type == TestVolumeType::Memory)
                .unwrap_or(&TEST_VOLUME_MEMORY)
                .clone(),
        ),
        (
            TestVolumeType::File,
            override_volumes
                .iter()
                .find(|volume| volume.volume_type == TestVolumeType::File)
                .unwrap_or(&TEST_VOLUME_FILE)
                .clone(),
        ),
        (
            TestVolumeType::S3,
            override_volumes
                .iter()
                .find(|volume| volume.volume_type == TestVolumeType::S3)
                .unwrap_or(&TEST_VOLUME_S3)
                .clone(),
        ),
        (
            TestVolumeType::S3Tables,
            override_volumes
                .iter()
                .find(|volume| volume.volume_type == TestVolumeType::S3Tables)
                .unwrap_or(&TEST_VOLUME_S3TABLES)
                .clone(),
        ),
    ]);

    // ignore errors when creating volume, as it could be created in previous run

    for VolumeConfig {
        volume_type,
        volume,
        database,
        prefix,
        ..
    } in used_volumes.values()
    {
        let volume = (*volume).to_string();
        match volume_type {
            TestVolumeType::Memory => {
                eprintln!("Creating memory volume: {volume}");
                let res = metastore
                    .create_volume(
                        &volume,
                        MetastoreVolume::new(volume.clone(), core_metastore::VolumeType::Memory),
                    )
                    .await;
                if let Err(e) = res {
                    eprintln!("Failed to create memory volume: {e}");
                }
            }
            TestVolumeType::File => {
                let mut user_data_dir = env::temp_dir();
                user_data_dir.push("store");
                user_data_dir.push(format!("user-volume-{suffix}"));
                let user_data_dir = user_data_dir.as_path();
                eprintln!("Creating file volume: {volume}, {user_data_dir:?}");
                let res = metastore
                    .create_volume(
                        &volume,
                        MetastoreVolume::new(
                            volume.clone(),
                            core_metastore::VolumeType::File(FileVolume {
                                path: user_data_dir.display().to_string(),
                            }),
                        ),
                    )
                    .await;
                if let Err(e) = res {
                    eprintln!("Failed to create file volume: {e}");
                }
            }
            TestVolumeType::S3 => {
                let prefix = prefix.unwrap_or(E2E_S3VOLUME_PREFIX);
                if let Ok(s3_volume) = s3_volume(prefix) {
                    eprintln!("Creating s3 volume: {volume}, {s3_volume:?}");
                    let res = metastore
                        .create_volume(
                            &volume,
                            MetastoreVolume::new(
                                volume.clone(),
                                core_metastore::VolumeType::S3(s3_volume),
                            ),
                        )
                        .await;
                    if let Err(e) = res {
                        eprintln!("Failed to create s3 volume: {e}");
                    }
                }
            }
            TestVolumeType::S3Tables => {
                let prefix = prefix.unwrap_or(E2E_S3TABLESVOLUME_PREFIX);
                if let Ok(s3_tables_volume) = s3_tables_volume(database, prefix) {
                    eprintln!("Creating s3tables volume: {volume}, {s3_tables_volume:?}");
                    let res = metastore
                        .create_volume(
                            &volume,
                            MetastoreVolume::new(
                                volume.clone(),
                                core_metastore::VolumeType::S3Tables(s3_tables_volume),
                            ),
                        )
                        .await;
                    if let Err(e) = res {
                        eprintln!("Failed to create s3tables volume: {e}");
                    }
                }
            }
        }
    }

    Ok(used_volumes)
}

#[derive(Debug, Clone)]
pub enum ObjectStoreType {
    Memory(String),            // suffix, not used by memory volume
    File(String, PathBuf),     // + suffix
    S3(String, S3ObjectStore), // + suffix
}

impl ObjectStoreType {
    pub fn suffix(&self) -> &str {
        match self {
            Self::Memory(suffix) | Self::File(suffix, ..) | Self::S3(suffix, ..) => suffix,
        }
    }
}

// Display
impl fmt::Display for ObjectStoreType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory(_) => write!(f, "Memory"),
            Self::File(suffix, path) => write!(f, "File({}/{suffix})", path.display()),
            Self::S3(suffix, s3_object_store) => write!(
                f,
                "S3({}/{suffix})",
                s3_object_store
                    .s3_builder
                    .get_config_value(&AmazonS3ConfigKey::Bucket)
                    .unwrap_or_default()
            ),
        }
    }
}

impl ObjectStoreType {
    #[allow(clippy::as_conversions)]
    pub fn object_store(&self) -> Result<Arc<dyn ObjectStore>, Error> {
        match &self {
            Self::Memory(_) => Ok(Arc::new(object_store::memory::InMemory::new())),
            Self::File(_, path, ..) => Ok(Arc::new(Self::object_store_at_path(path.as_path())?)),
            Self::S3(_, s3_object_store, ..) => s3_object_store
                .s3_builder
                .clone()
                .build()
                .map(|s3| Arc::new(s3) as Arc<dyn ObjectStore>)
                .context(TestObjectStoreSnafu),
        }
    }

    pub async fn db(&self) -> Result<Db, Error> {
        let db = match &self {
            Self::Memory(_) => Db::memory().await,
            Self::File(suffix, ..) | Self::S3(suffix, ..) => Db::new(Arc::new(
                DbBuilder::new(
                    object_store::path::Path::from(suffix.clone()),
                    self.object_store()?,
                )
                .build()
                .await
                .context(TestSlatedbSnafu {
                    object_store: self.object_store()?,
                })?,
            )),
        };

        Ok(db)
    }

    #[allow(clippy::unwrap_used, clippy::as_conversions)]
    pub fn object_store_at_path(path: &Path) -> Result<Arc<dyn ObjectStore>, Error> {
        if !path.exists() || !path.is_dir() {
            fs::create_dir(path).unwrap();
        }
        LocalFileSystem::new_with_prefix(path)
            .map(|fs| Arc::new(fs) as Arc<dyn ObjectStore>)
            .context(TestObjectStoreSnafu)
    }
}

pub async fn create_executor(
    object_store_type: ObjectStoreType,
    alias: &str,
) -> Result<ExecutorWithObjectStore, Error> {
    eprintln!("Creating executor with object store type: {object_store_type}");

    let db = object_store_type.db().await?;
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));
    let history_store = Arc::new(
        SlateDBHistoryStore::new_in_memory().await,
    );
    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        history_store.clone(),
        Arc::new(Config::default()),
    )
    .await
    .expect("Failed to create execution service");

    let exec = ExecutorWithObjectStore {
        executor: execution_svc,
        metastore: metastore.clone(),
        db: Arc::new(db),
        object_store_type: object_store_type.clone(),
        alias: alias.to_string(),
        // Here the place we normally create volumes
        used_volumes: create_volumes(metastore.clone(), &object_store_type, vec![]).await?,
    };

    exec.create_sessions().await?;

    Ok(exec)
}

// Support temporary option: early volumes creation for s3tables.
// TODO: Remove this function after adding EXTRANL VOLUME CREATING via sql
pub async fn create_executor_with_early_volumes_creation(
    object_store_type: ObjectStoreType,
    alias: &str,
    override_volumes: Vec<VolumeConfig>,
) -> Result<ExecutorWithObjectStore, Error> {
    eprintln!("Creating executor with object store type: {object_store_type}");

    let db = object_store_type.db().await?;
    let metastore = Arc::new(SlateDBMetastore::new(db.clone()));

    // create volumes before execution service is not a part of normal Embucket flow,
    // but we need it now to test s3 tables somehow
    let used_volumes =
        create_volumes(metastore.clone(), &object_store_type, override_volumes).await?;

    let history_store = Arc::new(
        SlateDBHistoryStore::new_in_memory().await,
    );
    let execution_svc = CoreExecutionService::new(
        metastore.clone(),
        history_store.clone(),
        Arc::new(Config::default()),
    )
    .await
    .context(TestExecutionSnafu {
        query: "EXECUTOR CREATE ERROR".to_string(),
    })?;

    let exec = ExecutorWithObjectStore {
        executor: execution_svc,
        metastore: metastore.clone(),
        db: Arc::new(db),
        object_store_type: object_store_type.clone(),
        alias: alias.to_string(),
        used_volumes,
    };

    exec.create_sessions().await?;

    Ok(exec)
}

// Every executor
#[allow(clippy::too_many_lines)]
pub async fn exec_parallel_test_plan(
    test_plan: Vec<ParallelTest>,
    volumes_databases_list: &[TestVolumeType],
) -> Result<bool, Error> {
    let mut passed = true;
    for volume_type in volumes_databases_list {
        // for VolumeConfig { volume, database, schema, .. } in &volumes_databases_list {
        for ParallelTest(tests) in &test_plan {
            // create sqls array here sql ref to String won't survive in the loop below
            let tests_sqls = tests
                .iter()
                .map(|TestQuery { sqls, executor, .. }| {
                    let VolumeConfig {
                        volume,
                        database,
                        schema,
                        ..
                    } = executor
                        .used_volumes
                        .get(volume_type)
                        .expect("VolumeConfig not found");
                    sqls.iter()
                        .map(|sql| prepare_statement(sql, volume, database, schema))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            // run batch sqls if any
            for (idx, test) in tests.iter().enumerate() {
                // get slice of all items except last
                let items = &tests_sqls[idx];
                let sync_items = &items[..items.len().saturating_sub(1)];

                // run synchronously all the queries except of last
                // these items are expected to pass
                for sql in sync_items {
                    let res = test
                        .executor
                        .executor
                        .query(test.session_id, sql, QueryContext::default())
                        .await
                        .context(TestExecutionSnafu { query: sql.clone() });
                    let ExecutorWithObjectStore {
                        alias,
                        object_store_type,
                        ..
                    } = test.executor.as_ref();
                    eprintln!(
                        "Exec synchronously with executor [{alias}], on object store: {object_store_type}, session: {}",
                        test.session_id
                    );
                    eprintln!("sql: {sql}\nres: {res:#?}");
                    res?;
                }
            }

            let mut parallel_runs = Vec::new();

            // run sqls concurrently
            let mut futures = Vec::new();
            for (idx, test) in tests.iter().enumerate() {
                // get slice of all items except last
                let items = &tests_sqls[idx];

                // run last item from every TestQuery in (non blocking mode)
                if let Some(sql) = items.last() {
                    futures.push(test.executor.executor.query(
                        test.session_id,
                        sql,
                        QueryContext::default(),
                    ));
                    parallel_runs.push((sql, test));
                }
            }

            let results = join_all(futures).await;

            for (idx, res) in results.into_iter().enumerate() {
                let (sql, test) = parallel_runs[idx];
                // let res = result.context(SnowflakeExecutionSnafu { query: sql.clone() });
                let res_is_ok = res.is_ok();
                let TestQuery {
                    err_callback,
                    expected_res,
                    session_id,
                    ..
                } = test;
                let test_num = idx + 1;
                let parallel_runs = parallel_runs.len();
                let ExecutorWithObjectStore {
                    alias,
                    object_store_type,
                    ..
                } = test.executor.as_ref();
                eprintln!(
                    "Exec concurrently with executor [{alias}], on object store: {object_store_type}, session: {session_id}"
                );
                eprintln!("sql {test_num}/{parallel_runs}: {sql}");
                match res {
                    Ok(res) => eprintln!("res: {res:#?}"),
                    Err(error) => {
                        eprintln!("Debug error: {error:#?}");
                        eprintln!("Chain error: {}", error.error_chain());
                        let snowflake_error = error.to_snowflake_error();
                        eprintln!("Snowflake debug error: {snowflake_error:#?}"); // message with line number in snowflake_errors
                        eprintln!("Snowflake display error: {snowflake_error}"); // clean message as from transport
                        // callback can fail on user's assertion
                        if let Some(err_callback) = err_callback {
                            err_callback.err_callback(&error);
                        }
                    }
                }

                eprintln!("expected_res: {expected_res}, actual_res: {res_is_ok}");
                if expected_res == &res_is_ok {
                    eprintln!("PASSED\n");
                } else {
                    eprintln!("FAILED\n");
                    passed = false;
                }
            }

            if !passed {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn prepare_statement(
    raw_statement: &str,
    volume_name: &str,
    database_name: &str,
    schema_name: &str,
) -> String {
    raw_statement
        .replace("__VOLUME__", volume_name)
        .replace("__DATABASE__", database_name)
        .replace("__SCHEMA__", schema_name)
}
