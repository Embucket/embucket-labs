{{config({
    "schema": "legacy"
  })
}}

WITH usage_ping AS (

    SELECT {{ hash_sensitive_columns('version_usage_data_source') }}
    FROM {{ ref('version_usage_data_source') }}

), hosts AS (

    SELECT DISTINCT
      host_id                             AS host_id,
      FIRST_VALUE(hostname) OVER (
          PARTITION BY host_id
          ORDER BY hostname IS NOT NULL DESC, 
                   created_at DESC
      )                                   AS host_name,
      uuid                                AS instance_id,
      source_ip_hash
    FROM usage_ping

), ip_to_country AS (

    SELECT *
    FROM {{ ref('map_ip_to_country') }}

), usage_with_ip AS (

    SELECT 
      hosts.*,
      ip_to_country.dim_location_country_id   AS location_id
    FROM hosts
    LEFT JOIN ip_to_country
      ON hosts.source_ip_hash = ip_to_country.ip_address_hash

), renamed AS (

    SELECT * 
    FROM usage_with_ip 

)


{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet",
    updated_by="@mcooperDD",
    created_date="2020-11-24",
    updated_date="2021-03-05"
) }}
