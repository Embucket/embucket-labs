 WITH source AS (

        SELECT * 
        FROM {{ source('sheetload','ta_team_assignment') }}

        )
        SELECT * 
        FROM source
