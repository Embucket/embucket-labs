WITH source AS (

    SELECT {{ nohash_sensitive_columns('marketo_activity_click_link_source', 'marketo_activity_click_link_id') }}
    FROM {{ ref('marketo_activity_click_link_source') }}

)

SELECT *
FROM source

