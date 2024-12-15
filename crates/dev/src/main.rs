use iceberg_rest_catalog::apis::configuration::Configuration;
use std::sync::Arc;
use arrow::array::RecordBatch;
use iceberg_rest_catalog::catalog::RestCatalog;
use iceberg_rust::catalog::bucket::ObjectStoreBuilder;
use object_store::local::LocalFileSystem;
use datafusion_iceberg::catalog::catalog::IcebergCatalog;
use datafusion::prelude::*;
use control_plane::sql::sql::SqlExecutor;
use control_plane::sql::functions::common::convert_record_batches;


#[tokio::main]
async fn main() {
    ////////////////////////////////
    //// Catalog configuration
    ////////////////////////////////

    let config = {
        let mut config = Configuration::new();
        config.base_path = "http://0.0.0.0:3000/catalog".to_string();
        config
    };
    let warehouse_id = "dd290519-8e5f-4426-82ca-a70d047f375b";
    let database_name = "test-namespace";
    let warehouse_name = "test-warehouse";
    let initial_table_ident = format!("`{warehouse_name}`.`{database_name}`.`initial_table`");
    let result_table_ident = format!("`{warehouse_name}`.`{database_name}`.`result_table`");
    let rest_client = RestCatalog::new(
        Some(warehouse_id),
        config.clone(),
        ObjectStoreBuilder::Filesystem(Arc::new(LocalFileSystem::new())),
    );
    let rest_client = Arc::new(rest_client);
    let catalog = IcebergCatalog::new(rest_client.clone(), None)
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog(warehouse_name, Arc::new(catalog));

    ////////// SETUP ////////

    // You can run setup once and then comment it out

    // let query1 = format!("CREATE TABLE {initial_table_ident} (i INTEGER, p CHAR(1), o INTEGER);");
    // let records: Vec<RecordBatch> = SqlExecutor::new(ctx.clone())
    //     .query(&query1.to_string(), &warehouse_name.clone().to_string())
    //     .await.unwrap()
    //     .into_iter()
    //     .collect::<Vec<_>>();
    // let records = convert_record_batches(records).unwrap();
    // println!("Query1 result: {records:?}");
    //
    // let query2 = format!("INSERT INTO {initial_table_ident} (i, p, o) VALUES
    //                             (1, 'A', 1),
    //                             (2, 'A', 2),
    //                             (3, 'B', 1),
    //                             (4, 'B', 2);");
    // let records: Vec<RecordBatch> = SqlExecutor::new(ctx.clone())
    //     .query(&query2.to_string(), &warehouse_name.clone().to_string())
    //     .await.unwrap()
    //     .into_iter()
    //     .collect::<Vec<_>>();
    // let records = convert_record_batches(records).unwrap();
    // println!("Query2 result: {records:?}");
    //
    // let query3 = format!("SELECT *
    //                             FROM (
    //                                  SELECT i, p, o,
    //                                         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS row_num
    //                                     FROM {initial_table_ident}
    //                                 )
    //                             WHERE row_num = 1;");
    // let records: Vec<RecordBatch> = SqlExecutor::new(ctx.clone())
    //     .query(&query3.to_string(), &warehouse_name.clone().to_string())
    //     .await.unwrap()
    //     .into_iter()
    //     .collect::<Vec<_>>();
    // let records = convert_record_batches(records).unwrap();
    // println!("Query3 result: {records:?}");

    ////////// SETUP END ////////

    // This is original query that we need to support
    // It fails with "Mismatch between schema and batches"
    // let query4 = format!("CREATE OR REPLACE table {result_table_ident} AS \
    //                             (WITH prep_table as (\
    //                                 SELECT i, p, o FROM {initial_table_ident} \
    //                                 WHERE p = 'A' OR p = 'B' \
    //                                 QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1)\
    //                                 SELECT p.i, p.p, p.o FROM prep_table p\
    //                              )");

    // This is the equivalent of query above but it`s natively supported by datafusion
    // It fails with "Mismatch between schema and batches"
    // let query4 = format!("CREATE OR REPLACE table {result_table_ident} AS \
    //                             (WITH prep_table as (\
    //                                 SELECT * FROM(
    //                                 SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) as qualify_alias \
    //                                 FROM {initial_table_ident} \
    //                                 WHERE p = 'A' OR p = 'B' ) r WHERE r.qualify_alias = 1)\
    //                                 SELECT p.i, p.p, p.o FROM prep_table p\
    //                              )");

    // This returns correct result
    // let query4 = format!("SELECT i, p, o \
    //                                 FROM {initial_table_ident}\
    //                                 ");

    // This returns correct result
    // let query4 = format!("SELECT i, p, o \
    //                                 FROM {initial_table_ident}\
    //                                 WHERE p = 'A' OR p = 'B'\
    //                                 ");

    // There is correct data in result, but it has weird shape, there is an empty record batch in the result
    // let query4 = format!("SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) as qualify_alias \
    //                                 FROM {initial_table_ident}\
    //                                 ");

    // There is correct data in result, but it has similar problems as query above but multiple empty batches
    let query4 = format!("SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) as \
    qualify_alias \
                                    FROM {initial_table_ident} \
                                    WHERE p = 'A' OR p = 'B'\
                                    ");
    let records: Vec<RecordBatch> = SqlExecutor::new(ctx.clone())
        .query(&query4.to_string(), &warehouse_name.clone().to_string())
        .await.unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    // println!("Query4 result BEFORE: {records:?}");
    let records = convert_record_batches(records).unwrap();
    println!("Query4 result AFTER: {records:?}");
}
