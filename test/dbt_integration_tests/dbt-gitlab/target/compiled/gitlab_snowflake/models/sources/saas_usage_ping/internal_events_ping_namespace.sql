

WITH base AS (

    SELECT *
    FROM EMBUCKET.saas_usage_ping.internal_events_namespace_metrics
    
    QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_ultimate_parent_id, ping_name, ping_date ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT
      md5(cast(coalesce(cast(namespace_ultimate_parent_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ping_name as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ping_date as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))           AS saas_usage_ping_gitlab_dotcom_namespace_id,
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