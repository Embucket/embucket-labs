WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_add_to_opportunity_source') }}

)

SELECT *
FROM source