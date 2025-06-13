WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_update_opportunity_source') }}

)

SELECT *
FROM source