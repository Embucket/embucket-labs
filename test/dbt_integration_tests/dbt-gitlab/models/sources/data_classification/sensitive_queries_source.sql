{{ config({
    "materialized": "incremental",
    "tags": ["data_classification", "mnpi_exception"],
    "unique_key":['classification_type', 'query_id'],
    "incremental_strategy":"delete+insert",
    "full_refresh": true if flags.FULL_REFRESH and var('full_refresh_force', false) else false
    })
}}

WITH sensitive_queries_base AS (

  SELECT *
  FROM {{ ref('sensitive_queries_base') }}

), grouped AS (

  SELECT sensitive_queries_base.classification_type::VARCHAR         AS classification_type,
         sensitive_queries_base.query_id::VARCHAR                    AS query_id,
         sensitive_queries_base.user_name::VARCHAR                   AS user_name,
         sensitive_queries_base.accessed_database::VARCHAR           AS accessed_database,
         sensitive_queries_base.accessed_schema::VARCHAR             AS accessed_schema,
         sensitive_queries_base.accessed_table::VARCHAR              AS accessed_table,
         sensitive_queries_base.query_text::VARCHAR                  AS query_text,
         sensitive_queries_base.executed_from_database_name::VARCHAR AS executed_from_database_name,
         sensitive_queries_base.executed_from_schema_name::VARCHAR   AS executed_from_schema_name,
         sensitive_queries_base.query_type::VARCHAR                  AS query_type,
         sensitive_queries_base.role_name::VARCHAR                   AS role_name,
         sensitive_queries_base.start_time::TIMESTAMP                AS start_time,
         sensitive_queries_base.end_time::TIMESTAMP                  AS end_time,
         CURRENT_TIMESTAMP()::TIMESTAMP                              AS _uploaded_at
  FROM sensitive_queries_base

  {% if is_incremental() %}
  --------------------------------------------------------------------------------
  -- ACCESS_HISTORY view has 3 hours lag and need to cover it to avoid data loss.
  -- Put 4 hours lag to be on the safe side
  -- Reference:
  -- https://docs.snowflake.com/en/sql-reference/account-usage/access_history
  --------------------------------------------------------------------------------
  WHERE sensitive_queries_base.start_time > (SELECT DATEADD(hour, -4, MAX(start_time)) FROM {{this}})

  {% endif %}
  --------------------------------------------------------------------------------
  -- Have GROUP BY ALL clause as will generate uniquer raw in the model
  -- in the model sensitive_queries_details will expose columns used for querying
  --------------------------------------------------------------------------------
  GROUP BY ALL

)

  SELECT *
  FROM grouped