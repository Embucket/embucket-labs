WITH source AS (

    SELECT {{ hash_sensitive_columns('marketo_activity_new_lead_source') }}
    FROM {{ ref('marketo_activity_new_lead_source') }}

)

SELECT *
FROM source