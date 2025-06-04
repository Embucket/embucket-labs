WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_abuse_top_download_data_source') }}

)

SELECT *
FROM source

