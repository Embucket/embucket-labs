{{ config(
    tags=["mnpi"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_net_arr_net_iacv_conversion_factors_source') }}

)

SELECT *
FROM source
