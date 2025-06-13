WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_change_owner_source') }}

)

SELECT *
FROM source