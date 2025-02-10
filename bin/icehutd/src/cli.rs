use clap::{Parser, ValueEnum};
use object_store::{
    aws::AmazonS3Builder, aws::S3ConditionalPut, local::LocalFileSystem, memory::InMemory,
    ObjectStore, Result as ObjectStoreResult,
};
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about, long_about=None)]
pub struct IceHutOpts {
    #[arg(
        short,
        long,
        value_enum,
        env = "OBJECT_STORE_BACKEND",
        help = "Backend to use for state storage"
    )]
    backend: StoreBackend,

    #[arg(
        long,
        env = "AWS_ACCESS_KEY_ID",
        required_if_eq("backend", "s3"),
        help = "AWS Access Key ID",
        help_heading = "S3 Backend Options"
    )]
    access_key_id: Option<String>,
    #[arg(
        long,
        env = "AWS_SECRET_ACCESS_KEY",
        required_if_eq("backend", "s3"),
        help = "AWS Secret Access Key",
        help_heading = "S3 Backend Options"
    )]
    secret_access_key: Option<String>,
    #[arg(
        long,
        env = "AWS_REGION",
        required_if_eq("backend", "s3"),
        help = "AWS Region",
        help_heading = "S3 Backend Options"
    )]
    region: Option<String>,
    #[arg(
        long,
        env = "S3_BUCKET",
        required_if_eq("backend", "s3"),
        help = "S3 Bucket Name",
        help_heading = "S3 Backend Options"
    )]
    bucket: Option<String>,
    #[arg(
        long,
        env = "S3_ENDPOINT",
        help = "S3 Endpoint (Optional)",
        help_heading = "S3 Backend Options"
    )]
    endpoint: Option<String>,
    #[arg(
        long,
        env = "S3_ALLOW_HTTP",
        help = "Allow HTTP for S3 (Optional)",
        help_heading = "S3 Backend Options"
    )]
    allow_http: Option<bool>,

    #[arg(long, env="FILE_STORAGE_PATH", 
            required_if_eq("backend", "file"),
            conflicts_with_all(["region", "bucket", "endpoint", "allow_http"]),
            help_heading="File Backend Options",
            help="Path to the directory where files will be stored"
        )]
    file_storage_path: Option<PathBuf>,

    #[arg(short, long, env = "SLATEDB_PREFIX")]
    pub slatedb_prefix: String,

    #[arg(
        long,
        env = "ICEHUT_HOST",
        default_value = "127.0.0.1",
        help = "Host to bind to"
    )]
    pub host: Option<String>,

    #[arg(
        long,
        env = "ICEHUT_PORT",
        default_value = "3000",
        help = "Port to bind to"
    )]
    pub port: Option<u16>,

    #[arg(long, default_value = "true")]
    use_fs: Option<bool>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum StoreBackend {
    S3,
    File,
    Memory,
}

impl IceHutOpts {
    #[allow(clippy::unwrap_used, clippy::as_conversions)]
    pub fn object_store_backend(self) -> ObjectStoreResult<Box<dyn ObjectStore>> {
        // TODO: Hacky workaround for now, need to figure out a better way to pass this
        // TODO: Really, seriously remove this. This is a hack.
        unsafe {
            let use_fs = self.use_fs.unwrap_or(false);
            std::env::set_var("USE_FILE_SYSTEM_INSTEAD_OF_CLOUD", use_fs.to_string());
        }
        match self.backend {
            StoreBackend::S3 => {
                let s3_allow_http = self.allow_http.unwrap_or(false);

                let s3_builder = AmazonS3Builder::new()
                    .with_access_key_id(self.access_key_id.unwrap())
                    .with_secret_access_key(self.secret_access_key.unwrap())
                    .with_region(self.region.unwrap())
                    .with_bucket_name(self.bucket.unwrap())
                    .with_conditional_put(S3ConditionalPut::ETagMatch);

                if let Some(endpoint) = self.endpoint {
                    s3_builder
                        .with_endpoint(&endpoint)
                        .with_allow_http(s3_allow_http)
                        .build()
                        .map(|s3| Box::new(s3) as Box<dyn ObjectStore>)
                } else {
                    s3_builder
                        .build()
                        .map(|s3| Box::new(s3) as Box<dyn ObjectStore>)
                }
            }
            StoreBackend::File => {
                let file_storage_path = self.file_storage_path.unwrap();
                let path = file_storage_path.as_path();
                if !path.exists() || !path.is_dir() {
                    fs::create_dir(path).unwrap();
                }
                LocalFileSystem::new_with_prefix(file_storage_path)
                    .map(|fs| Box::new(fs) as Box<dyn ObjectStore>)
            }
            StoreBackend::Memory => Ok(Box::new(InMemory::new()) as Box<dyn ObjectStore>),
        }
    }
}
