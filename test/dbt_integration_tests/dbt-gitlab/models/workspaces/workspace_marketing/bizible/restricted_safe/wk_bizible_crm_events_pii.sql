WITH source AS (

    SELECT *
    FROM {{ ref('bizible_crm_events_source_pii') }}

)

SELECT *
FROM source