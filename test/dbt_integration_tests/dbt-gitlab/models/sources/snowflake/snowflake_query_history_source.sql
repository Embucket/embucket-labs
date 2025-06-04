{{
  config(
    materialized='incremental',
    unique_key='query_id',
    on_schema_change='append_new_columns'
  )
}}


WITH source AS (

  SELECT *
  FROM {{ source('snowflake_account_usage','query_history') }}
  {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    WHERE end_time > (SELECT MAX(query_end_at) FROM {{ this }})

  {% endif %}

),

renamed AS (

  SELECT
    query_id::VARCHAR AS query_id,
    query_text::VARCHAR AS query_text,
    database_id::NUMBER AS database_id,
    database_name::VARCHAR AS database_name,
    schema_id::NUMBER AS schema_id,
    schema_name::VARCHAR AS schema_name,
    query_type::VARCHAR AS query_type,
    session_id::NUMBER AS session_id,
    user_name::VARCHAR AS user_name,
    role_name::VARCHAR AS role_name,
    warehouse_id::NUMBER AS warehouse_id,
    warehouse_name::VARCHAR AS warehouse_name,
    warehouse_size::VARCHAR AS warehouse_size,  -- noqa: L029
    warehouse_type::VARCHAR AS warehouse_type,
    cluster_number::VARCHAR AS cluster_number,
    query_tag::VARCHAR AS query_tag,
    execution_status::VARCHAR AS execution_status,
    error_code::VARCHAR AS error_code,
    error_message::VARCHAR AS error_message,
    start_time::TIMESTAMP AS query_start_at,
    end_time::TIMESTAMP AS query_end_at,
    total_elapsed_time::NUMBER AS total_elapsed_time,
    bytes_scanned::NUMBER AS bytes_scanned,
    percentage_scanned_from_cache::NUMBER AS percentage_scanned_from_cache,
    bytes_written::NUMBER AS bytes_written,
    bytes_written_to_result::NUMBER AS bytes_written_to_result,
    bytes_read_from_result::NUMBER AS bytes_read_from_result,
    rows_produced::NUMBER AS rows_produced,
    rows_inserted::NUMBER AS rows_inserted,
    rows_updated::NUMBER AS rows_updated,
    rows_deleted::NUMBER AS rows_deleted,
    rows_unloaded::NUMBER AS rows_unloaded,
    bytes_deleted::NUMBER AS bytes_deleted,
    partitions_scanned::NUMBER AS partitions_scanned,
    partitions_total::NUMBER AS partitions_total,
    bytes_spilled_to_local_storage::NUMBER AS bytes_spilled_to_local_storage,
    bytes_spilled_to_remote_storage::NUMBER AS bytes_spilled_to_remote_storage,
    bytes_sent_over_the_network::NUMBER AS bytes_sent_over_the_network,
    compilation_time::NUMBER AS compilation_time,
    execution_time::NUMBER AS execution_time,
    queued_provisioning_time::NUMBER AS queued_provisioning_time,
    queued_repair_time::NUMBER AS queued_repair_time,
    queued_overload_time::NUMBER AS queued_overload_time,
    transaction_blocked_time::NUMBER AS transaction_blocked_time,
    outbound_data_transfer_cloud::VARCHAR AS outbound_data_transfer_cloud,
    outbound_data_transfer_region::VARCHAR AS outbound_data_transfer_region,
    outbound_data_transfer_bytes::NUMBER AS outbound_data_transfer_bytes,
    inbound_data_transfer_cloud::VARCHAR AS inbound_data_transfer_cloud,
    inbound_data_transfer_region::VARCHAR AS inbound_data_transfer_region,
    inbound_data_transfer_bytes::NUMBER AS inbound_data_transfer_bytes,
    list_external_files_time::NUMBER AS list_external_files_time,
    credits_used_cloud_services::NUMBER AS credits_used_cloud_services,
    release_version::VARCHAR AS release_version,
    external_function_total_invocations::NUMBER AS external_function_total_invocations,
    external_function_total_sent_rows::NUMBER AS external_function_total_sent_rows,
    external_function_total_received_rows::NUMBER AS external_function_total_received_rows,
    external_function_total_sent_bytes::NUMBER AS external_function_total_sent_bytes,
    external_function_total_received_bytes::NUMBER AS external_function_total_received_bytes,
    query_load_percent::NUMBER AS query_load_percent,
    is_client_generated_statement::BOOLEAN AS is_client_generated_statement,
    query_acceleration_bytes_scanned::NUMBER AS query_acceleration_bytes_scanned,
    query_acceleration_partitions_scanned::NUMBER AS query_acceleration_partitions_scanned,
    query_acceleration_upper_limit_scale_factor::NUMBER
    AS query_acceleration_upper_limit_scale_factor

  FROM source

)

SELECT *
FROM renamed
