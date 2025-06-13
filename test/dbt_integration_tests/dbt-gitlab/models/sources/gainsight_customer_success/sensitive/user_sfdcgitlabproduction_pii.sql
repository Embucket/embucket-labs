{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

  SELECT {{ nohash_sensitive_columns('user_sfdcgitlabproduction','sfdcuser_name') }}
  FROM {{ref('user_sfdcgitlabproduction')}}

)

SELECT *
FROM source

