{{ config({
        "materialized": "incremental",
        "unique_key": "ip_address_hash",
    })
}}

WITH all_hashed_ips_version_usage AS (

    SELECT
      {{ nohash_sensitive_columns('version_usage_data_source', 'source_ip') }}
    FROM {{ ref('version_usage_data_source') }}

),  all_distinct_ips AS (

    SELECT DISTINCT
      source_ip_hash,
      PARSE_IP(source_ip, 'inet')['ip_fields'][0]::NUMBER AS source_ip_numeric
    FROM all_hashed_ips_version_usage
    {% if is_incremental() %}
        WHERE source_ip_hash NOT IN (
            SELECT
              ip_address_hash
            FROM {{this}}
        )
    {% endif %}

), maxmind_ip_ranges AS (

   SELECT *
   FROM {{ ref('sheetload_maxmind_ip_ranges_source') }}

), newly_mapped_ips AS (

    SELECT
      source_ip_hash    AS ip_address_hash,
      geoname_id        AS dim_location_country_id
    FROM all_distinct_ips
    JOIN maxmind_ip_ranges
    WHERE all_distinct_ips.source_ip_numeric BETWEEN maxmind_ip_ranges.ip_range_first_ip_numeric AND maxmind_ip_ranges.ip_range_last_ip_numeric

)

{{ dbt_audit(
    cte_ref="newly_mapped_ips",
    created_by="@m_walker",
    updated_by="@mcooperDD",
    created_date="2020-08-25",
    updated_date="2020-03-05"
) }}
