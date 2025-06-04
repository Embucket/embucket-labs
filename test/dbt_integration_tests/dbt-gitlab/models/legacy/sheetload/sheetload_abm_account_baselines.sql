{{ config({
    "schema": "legacy"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_abm_account_baselines_source') }}

)

SELECT *
FROM source
