WITH source AS (

    SELECT {{ hash_sensitive_columns('marketo_activity_visit_webpage_source') }}
    FROM {{ ref('marketo_activity_visit_webpage_source') }}

)

SELECT *
FROM source
