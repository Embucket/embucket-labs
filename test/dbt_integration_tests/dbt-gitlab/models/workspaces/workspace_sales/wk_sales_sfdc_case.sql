{{ config(alias='sfdc_case') }}


WITH source AS (

  SELECT *
  FROM {{ ref('sfdc_case_source') }}
  WHERE is_deleted = FALSE

)

SELECT *
FROM source
