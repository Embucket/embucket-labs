{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

  SELECT {{ nohash_sensitive_columns('companies_and_ebrs','cta_owner_name') }}
  FROM {{ref('companies_and_ebrs')}}

)

SELECT *
FROM source

