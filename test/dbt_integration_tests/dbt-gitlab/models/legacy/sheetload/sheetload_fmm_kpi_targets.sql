WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_fmm_kpi_targets_source') }}

)

SELECT *
FROM source
