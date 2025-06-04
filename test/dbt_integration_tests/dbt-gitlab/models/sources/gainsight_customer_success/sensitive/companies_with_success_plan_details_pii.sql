{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

  SELECT {{ nohash_sensitive_columns('companies_with_success_plan_details','tam_name') }}
  FROM {{ref('companies_with_success_plan_details')}}

)

SELECT *
FROM source

