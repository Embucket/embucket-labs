WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_deleted_mrs_source') }}

)

SELECT *
FROM source
