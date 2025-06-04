{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

  SELECT {{ nohash_sensitive_columns('email_log_v2', 'gs_id') }}
  FROM {{ref('email_log_v2')}}

)

SELECT *
FROM source