WITH source AS (

    SELECT *
    FROM {{ ref('incident_history_source') }}

)

SELECT *
FROM source
