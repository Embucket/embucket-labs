WITH source AS (

    SELECT {{ hash_sensitive_columns('marketo_activity_click_link_source') }}
    FROM {{ ref('marketo_activity_click_link_source') }}

)

SELECT *
FROM source