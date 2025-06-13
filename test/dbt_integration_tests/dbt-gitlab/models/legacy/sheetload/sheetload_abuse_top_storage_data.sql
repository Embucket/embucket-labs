WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_abuse_top_storage_data_source') }}

)

SELECT *
FROM source

