

WITH base AS (

    SELECT *
    FROM {{ source('saas_usage_ping', 'instance_sql_errors') }}

), partitioned AS (

    SELECT 
      run_id       AS run_id,
      sql_errors   AS sql_errors,
      ping_date    AS ping_date,
      _uploaded_at AS uploaded_at
    FROM base

), renamed AS (

    SELECT
      run_id                                              AS run_id,
      TRY_PARSE_JSON(sql_errors)                          AS sql_errors,
      ping_date::TIMESTAMP                                AS ping_date,
      DATEADD('s', uploaded_at, '1970-01-01')::TIMESTAMP  AS uploaded_at
    FROM partitioned

)

SELECT *
FROM renamed

