{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

  SELECT {{ nohash_sensitive_columns('activity_timeline','user_email') }}
  FROM {{ref('activity_timeline')}}

)

SELECT *
FROM source
