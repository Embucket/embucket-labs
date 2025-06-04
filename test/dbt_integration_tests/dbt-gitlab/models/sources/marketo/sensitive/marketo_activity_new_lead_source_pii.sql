WITH source AS (

    SELECT {{ nohash_sensitive_columns('marketo_activity_new_lead_source', 'marketo_activity_new_lead_id') }}
    FROM {{ ref('marketo_activity_new_lead_source') }}

)

SELECT *
FROM source