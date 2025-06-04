WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_click_link_source_pii') }}

)

SELECT *
FROM source

