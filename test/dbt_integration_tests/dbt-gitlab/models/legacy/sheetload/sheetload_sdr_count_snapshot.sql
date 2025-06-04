WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sdr_count_snapshot_source') }}

)

SELECT *
FROM source
