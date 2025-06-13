{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

  SELECT {{ nohash_sensitive_columns('advanced_outreach_participants','account_name') }}
  FROM {{ref('advanced_outreach_participants')}}

)

SELECT *
FROM source

