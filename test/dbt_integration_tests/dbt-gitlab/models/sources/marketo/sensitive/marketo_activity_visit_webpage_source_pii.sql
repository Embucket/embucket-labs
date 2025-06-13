WITH source AS (

    SELECT {{ nohash_sensitive_columns('marketo_activity_visit_webpage_source', 'marketo_activity_visit_webpage_id') }}
    FROM {{ ref('marketo_activity_visit_webpage_source') }}

)

SELECT *
FROM source
