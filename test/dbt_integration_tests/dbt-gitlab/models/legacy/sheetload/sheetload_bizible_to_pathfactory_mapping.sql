WITH source AS (

        SELECT * 
        FROM {{ ref('sheetload_bizible_to_pathfactory_mapping_source') }}

        )
        SELECT * 
        FROM source