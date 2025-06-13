WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_sfdc_activity_source_pii') }}

)

SELECT *
FROM source