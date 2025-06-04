WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_territory_mapping_source') }}

)

SELECT *
FROM source
