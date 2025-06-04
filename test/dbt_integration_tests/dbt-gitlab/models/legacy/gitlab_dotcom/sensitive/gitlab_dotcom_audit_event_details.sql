{{ config({
        "materialized": "incremental",
        "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_audit_events_source') }}
  {% if is_incremental() %}
  WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})
  {% endif %}

), details_parsed AS (

    SELECT
      audit_event_id,
      REGEXP_SUBSTR(audit_event_details, '(.*):(.*)', 1, 1 ,'c',1) AS key_name,
      REGEXP_SUBSTR(audit_event_details, '(.*): (.*)', 1, 1 ,'c',2) AS key_value,
      created_at
    FROM source
    WHERE key_name IS NOT NULL

)

SELECT *
FROM details_parsed
