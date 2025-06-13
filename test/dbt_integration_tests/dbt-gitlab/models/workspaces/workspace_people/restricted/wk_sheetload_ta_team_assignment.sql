WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_ta_team_assignment_source') }}

)

SELECT *
FROM source