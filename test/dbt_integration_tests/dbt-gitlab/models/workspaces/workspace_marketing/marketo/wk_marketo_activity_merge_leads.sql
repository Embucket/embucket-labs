WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_merge_leads_source') }}

)

SELECT *
FROM source