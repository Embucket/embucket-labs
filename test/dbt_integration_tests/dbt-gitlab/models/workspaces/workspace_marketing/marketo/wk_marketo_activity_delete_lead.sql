WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_delete_lead_source') }}

)

SELECT *
FROM source