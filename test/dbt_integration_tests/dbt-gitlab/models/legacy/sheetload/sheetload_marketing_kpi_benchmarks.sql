{{ config({
    "schema": "legacy"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_marketing_kpi_benchmarks_source') }}

)

SELECT *
FROM source
