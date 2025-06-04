WITH source AS (

    SELECT {{ hash_sensitive_columns('marketo_activity_push_lead_to_marketo_source') }}
    FROM {{ ref('marketo_activity_push_lead_to_marketo_source') }}

)

SELECT *
FROM source