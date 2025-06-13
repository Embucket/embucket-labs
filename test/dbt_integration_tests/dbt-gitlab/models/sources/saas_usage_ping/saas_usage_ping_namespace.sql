{{ config({
    "materialized": "incremental",
    "unique_key": "saas_usage_ping_gitlab_dotcom_namespace_id",
    "tmp_relation_type": "table",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

WITH base AS (

    SELECT *
    FROM {{ source('saas_usage_ping', 'namespace') }}
    {% if is_incremental() %}

    WHERE DATEADD('s', _uploaded_at, '1970-01-01') >= (SELECT MAX(_uploaded_at) FROM {{this}})

    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_ultimate_parent_id, ping_name, ping_date ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key( ['namespace_ultimate_parent_id',
                                    'ping_name',
                                    'ping_date'])}}           AS saas_usage_ping_gitlab_dotcom_namespace_id,
      namespace_ultimate_parent_id::INT                       AS namespace_ultimate_parent_id,
      counter_value::INT                                      AS counter_value,
      ping_name::VARCHAR                                      AS ping_name,
      level::VARCHAR                                          AS ping_level,
      query_ran::VARCHAR                                      AS query_ran,
      error::VARCHAR                                          AS error,
      ping_date::TIMESTAMP                                    AS ping_date,
      dateadd('s', _uploaded_at, '1970-01-01')::TIMESTAMP     AS _uploaded_at
    FROM base


)

SELECT *
FROM renamed
