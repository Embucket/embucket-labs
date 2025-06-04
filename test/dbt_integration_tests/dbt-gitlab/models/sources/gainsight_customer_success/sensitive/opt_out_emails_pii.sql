{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

  SELECT {{ nohash_sensitive_columns('opt_out_emails','email_address') }}
  FROM {{ref('opt_out_emails')}}

)

SELECT *
FROM source

