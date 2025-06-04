{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

  SELECT {{ nohash_sensitive_columns('email_logs','email_address') }}
  FROM {{ref('email_logs')}}

)

SELECT *
FROM source

