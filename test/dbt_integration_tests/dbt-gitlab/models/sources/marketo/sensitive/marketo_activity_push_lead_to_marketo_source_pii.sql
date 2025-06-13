WITH source AS (

    SELECT {{ nohash_sensitive_columns('marketo_activity_push_lead_to_marketo_source', 'marketo_activity_push_lead_to_marketo_id') }}
    FROM {{ ref('marketo_activity_push_lead_to_marketo_source') }}

)

SELECT *
FROM source

