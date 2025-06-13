WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sdr_adaptive_data_source') }}

)

SELECT *
FROM source
