{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

  SELECT {{ nohash_sensitive_columns('advanced_outreach_emails','from_address') }}
  FROM {{ref('advanced_outreach_emails')}}

)

SELECT *
FROM source
