use aws_config;
use aws_config::Region;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3tables::config::Credentials;
use aws_sdk_s3tables::config::SharedCredentialsProvider;
use aws_sdk_s3tables::operation::list_tables::ListTablesOutput;
use aws_sdk_s3tables::{Client, Config, Error};
use aws_sdk_sts::Client as StsClient;
use std::collections::HashMap;

// not yet working
pub async fn s3_role_client(
    access_key_id: String,
    secret_access_key: String,
    region: String,
    account_id: String,
) -> Client {
    let role_name = "e2e-s3-tables";

    let creds = Credentials::builder()
        .access_key_id(access_key_id.clone())
        .secret_access_key(secret_access_key.clone())
        .account_id(account_id.clone())
        .provider_name("test")
        .build();

    let sts_config = aws_config::from_env()
        .credentials_provider(creds.clone())
        .load()
        .await;

    let sts_client = StsClient::new(&sts_config);
    let assumed_role = sts_client
        .assume_role()
        .role_arn(format!("arn:aws:sts::{account_id}:role/{role_name}"))
        .role_session_name(format!(
            "e2e_tests_session_{}",
            chrono::Utc::now().timestamp()
        ))
        .send()
        .await;
    eprintln!("Assumed role: {assumed_role:?}");

    let assumed_creds = Credentials::from_keys(access_key_id, secret_access_key, None);

    let region_provider = RegionProviderChain::first_try(Region::new(region));

    let config = aws_config::from_env()
        .credentials_provider(assumed_creds)
        .region(region_provider)
        .load()
        .await;
    Client::new(&config)
}

pub async fn s3_client(
    access_key_id: String,
    secret_access_key: String,
    region: String,
    account_id: String,
) -> Client {
    let creds = Credentials::builder()
        .access_key_id(access_key_id)
        .secret_access_key(secret_access_key)
        .account_id(account_id)
        .provider_name("test")
        .build();

    let config = Config::builder()
        .credentials_provider(SharedCredentialsProvider::new(creds))
        .region(Region::new(region))
        .build();
    Client::from_conf(config)
}

pub async fn get_s3tables_bucket_tables(
    client: &Client,
    arn: String,
) -> Result<ListTablesOutput, Error> {
    let result = client.list_tables().table_bucket_arn(arn).send().await;

    match result {
        Ok(res) => {
            eprintln!("get_s3tables_bucket_tables OK");
            Ok(res)
        }
        Err(e) => Err(e.into_service_error().into()),
    }
}

pub async fn get_s3tables_tables_arns_map(
    client: &Client,
    bucket_arn: String,
) -> Result<HashMap<String, String>, Error> {
    // get tables arns to assign policies
    let tables = get_s3tables_bucket_tables(client, bucket_arn).await?;
    let tables: HashMap<String, String> = tables
        .tables
        .iter()
        .map(|table| {
            (
                format!("{}.{}", table.namespace.join("."), table.name.clone()),
                table.table_arn.clone(),
            )
        })
        .collect();

    Ok(tables)
}

pub async fn set_table_bucket_policy(
    client: &Client,
    arn: String,
    policy: String,
) -> Result<(), Error> {
    eprintln!("set_table_bucket_policy: arn: {arn}, policy: {policy}");
    let result = client
        .put_table_bucket_policy()
        .table_bucket_arn(arn)
        .resource_policy(policy)
        .send()
        .await;

    match result {
        Ok(_) => {
            eprintln!("set_table_bucket_policy OK");
            Ok(())
        }
        Err(e) => Err(e.into_service_error().into()),
    }
}

pub async fn set_s3table_bucket_table_policy(
    client: &Client,
    arn: String,
    namespace: String,
    table_name: String,
    policy: String,
) -> Result<(), Error> {
    eprintln!(
        "set_s3table_bucket_table_policy: arn: {arn}, namespace: {namespace}, table_name: {table_name}, policy: {policy}"
    );
    let result = client
        .put_table_policy()
        .table_bucket_arn(arn)
        .namespace(namespace)
        .name(table_name)
        .resource_policy(policy)
        .send()
        .await;

    match result {
        Ok(_) => {
            eprintln!("set_s3table_bucket_table_policy OK");
            Ok(())
        }
        Err(e) => Err(e.into_service_error().into()),
    }
}

pub async fn delete_s3tables_bucket_table(
    client: &Client,
    arn: String,
    namespace: String,
    table_name: String,
) -> Result<(), Error> {
    eprintln!(
        "delete_s3tables_bucket_table: arn: {arn}, namespace: {namespace}, table_name: {table_name}"
    );
    let result = client
        .delete_table()
        .table_bucket_arn(arn)
        .namespace(namespace)
        .name(table_name)
        .send()
        .await;

    match result {
        Ok(_) => Ok(()),
        Err(e) => Err(e.into_service_error().into()),
    }
}

pub async fn delete_s3tables_bucket_table_policy(
    client: &Client,
    arn: String,
    namespace: String,
    table_name: String,
) -> Result<(), Error> {
    eprintln!(
        "delete_s3tables_bucket_table_policy: arn: {arn}, namespace: {namespace}, table_name: {table_name}"
    );
    let result = client
        .delete_table_policy()
        .table_bucket_arn(arn)
        .namespace(namespace)
        .name(table_name)
        .send()
        .await;

    match result {
        Ok(_) => Ok(()),
        Err(e) => Err(e.into_service_error().into()),
    }
}

pub async fn delete_table_bucket_policy(client: &Client, arn: String) -> Result<(), Error> {
    eprintln!("delete_table_bucket_policy: arn: {arn}");
    let result = client
        .delete_table_bucket_policy()
        .table_bucket_arn(arn)
        .send()
        .await;

    match result {
        Ok(_) => Ok(()),
        Err(e) => Err(e.into_service_error().into()),
    }
}

pub async fn create_s3tables_namespace(
    client: &Client,
    arn: String,
    namespace: String,
) -> Result<(), Error> {
    eprintln!("create_s3tables_namespace: arn: {arn}, namespace: {namespace}");
    let result = client
        .create_namespace()
        .table_bucket_arn(arn)
        .namespace(namespace)
        .send()
        .await;

    match result {
        Ok(_) => Ok(()),
        Err(e) => Err(e.into_service_error().into()),
    }
}
