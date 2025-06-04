WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_ar_aging_details_source') }}

)

SELECT *
FROM source
