WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_abuse_top_ci_data_source') }}

)

SELECT *
FROM source

