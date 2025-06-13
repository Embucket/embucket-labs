WITH source AS (

    SELECT {{ nohash_sensitive_columns('marketo_activity_sfdc_activity_source', 'marketo_activity_sfdc_activity_id') }}
    FROM {{ ref('marketo_activity_sfdc_activity_source') }}

)

SELECT *
FROM source
