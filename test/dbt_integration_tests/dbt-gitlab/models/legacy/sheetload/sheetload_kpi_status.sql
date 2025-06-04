WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_kpi_status_source') }}

)

SELECT *
FROM source
