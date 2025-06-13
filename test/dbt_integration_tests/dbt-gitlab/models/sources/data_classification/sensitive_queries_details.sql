{{ config({
    "materialized": "incremental",
    "tags": ["data_classification", "mnpi_exception"],
    "unique_key":['classification_type', 'query_id', 'accessed_column', 'tag_name'],
    "incremental_strategy":"delete+insert",
    "full_refresh": true if flags.FULL_REFRESH and var('full_refresh_force', false) else false
    })
}}

WITH sensitive_queries_base AS (

  SELECT *
  FROM {{ ref('sensitive_queries_base') }}

  {% if is_incremental() %}
    --------------------------------------------------------------------------------
    -- ACCESS_HISTORY view has 3 hours lag and need to cover it to avoid data loss.
    -- Put 4 hours lag to be on the safe side
    -- Reference:
    -- https://docs.snowflake.com/en/sql-reference/account-usage/access_history
    --------------------------------------------------------------------------------
    WHERE start_time > (SELECT DATEADD(hour, -4, MAX(start_time)) FROM {{this}})

  {% endif %}

), details AS (

  SELECT sensitive_queries_base.classification_type::VARCHAR  AS classification_type,
         sensitive_queries_base.query_id::VARCHAR             AS query_id,
         sensitive_queries_base.accessed_column::VARCHAR      AS accessed_column,
         sensitive_queries_base.tag_name::VARCHAR             AS tag_name,
         sensitive_queries_base.tag_value::VARCHAR            AS tag_value,
         start_time::TIMESTAMP                                AS start_time,
         CURRENT_TIMESTAMP()::TIMESTAMP                       AS _uploaded_at
  FROM sensitive_queries_base



)

  SELECT *
  FROM details