{{ config(
  enabled=false
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('zi_reference_techs_source') }}

)

SELECT *
FROM source