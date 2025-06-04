{{ config(
    tags=["mnpi"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_manual_arr_true_up_allocation_source') }}

)

SELECT *
FROM source