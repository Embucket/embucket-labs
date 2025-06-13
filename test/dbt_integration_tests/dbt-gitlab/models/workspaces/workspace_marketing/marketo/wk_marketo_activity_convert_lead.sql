WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_convert_lead_source') }}

)

SELECT *
FROM source