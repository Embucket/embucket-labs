{{ config(
    materialized='incremental',
    unique_key='primary_key',
    on_schema_change='append_new_columns',
    full_refresh=only_force_full_refresh() 
    )
}}

WITH
source AS (

  SELECT
    OBJECT_DELETE(PARSE_JSON(value), 'gcs_export_time') AS modified_value,
    OBJECT_INSERT(modified_value,'credits', ARRAY_SORT(value['credits']::VARIANT),TRUE) AS sorted_credits,
    OBJECT_INSERT(sorted_credits, 'labels',  ARRAY_SORT(value['labels']::VARIANT), TRUE) AS sorted_labels,
    OBJECT_INSERT(sorted_labels, 'project::ancestors', ARRAY_SORT(value['project']['ancestors']::VARIANT), TRUE) AS sorted_project_ancestors,
    OBJECT_INSERT(sorted_project_ancestors, 'project::labels', ARRAY_SORT(value['project']['labels']::VARIANT), TRUE) AS sorted_project_labels,
    OBJECT_INSERT(sorted_project_labels, 'system_labels',  ARRAY_SORT(value['system_labels']::VARIANT), TRUE) AS sorted_system_labels,
    TO_TIMESTAMP(value['gcs_export_time']::INT, 6) AS gcs_export_time
  FROM {{ source('gcp_billing','detail_gcp_billing') }}
  {% if is_incremental() %}

  WHERE date_part > (SELECT MAX({{ var('incremental_backfill_date', 'partition_date') }}) FROM {{ this }})
    AND date_part <= (SELECT DATEADD(MONTH, 1, MAX({{ var('incremental_backfill_date', 'partition_date') }})) FROM {{ this }})
    AND gcs_export_time > (SELECT MAX(uploaded_at) FROM {{ this }})

  {% else %}
  -- This will cover the first creation of the table or a full refresh and requires that the table be backfilled
  WHERE date_part > DATEADD('day', -30 ,CURRENT_DATE())

  {% endif %}

),

grouped AS (
  
  SELECT
    sorted_system_labels AS value,
    gcs_export_time,
    count(*) AS occurrence_multiplier
  FROM source
  GROUP BY 1,2

),

renamed AS (
  
  SELECT
    {{ dbt_utils.generate_surrogate_key(['value']) }} as primary_key,
    value['billing_account_id']::VARCHAR AS billing_account_id,
    value['cost']::FLOAT * occurrence_multiplier AS cost,
    value['cost_type']::VARCHAR AS cost_type,
    value['credits']::VARIANT AS credits,
    value['currency']::VARCHAR AS currency,
    value['currency_conversion_rate']::FLOAT AS currency_conversion_rate,
    TO_TIMESTAMP(value['export_time']::INT, 6) AS export_time,
    TO_DATE(value['invoice']['month']::STRING, 'YYYYMM') AS invoice_month,
    value['labels']::VARIANT AS labels,
    value['location']['country']::VARCHAR AS resource_country,
    value['location']['location']::VARCHAR AS resource_location,
    value['location']['region']::VARCHAR AS resource_region,
    value['location']['zone']::VARCHAR AS resource_zone,
    value['project']['ancestors']::VARIANT AS project_ancestors,
    value['project']['ancestry_numbers']::VARCHAR AS folder_id,
    value['project']['id']::VARCHAR AS project_id,
    value['project']['labels']::VARIANT AS project_labels,
    value['project']['name']::VARCHAR AS project_name,
    value['resource']['global_name']::VARCHAR AS resource_global_name,
    value['resource']['name']::VARCHAR AS resource_name,
    value['service']['id']::VARCHAR AS service_id,
    value['service']['description']::VARCHAR AS service_description,
    value['sku']['id']::VARCHAR AS sku_id,
    value['sku']['description']::VARCHAR AS sku_description,
    value['system_labels']::VARIANT AS system_labels,
    value['usage']['pricing_unit']::VARCHAR AS pricing_unit,
    value['usage']['amount']::FLOAT * occurrence_multiplier AS usage_amount,
    value['usage']['amount_in_pricing_units']::FLOAT * occurrence_multiplier AS usage_amount_in_pricing_units,
    value['usage']['unit']::VARCHAR AS usage_unit,
    TO_TIMESTAMP(value['usage_start_time']::INT, 6) AS usage_start_time,
    TO_TIMESTAMP(value['usage_end_time']::INT, 6) AS usage_end_time,
    value['_partition_date']::DATE AS partition_date,
    gcs_export_time AS uploaded_at,
    occurrence_multiplier
  
  
  FROM grouped


)

SELECT *
FROM renamed