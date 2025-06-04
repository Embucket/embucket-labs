WITH source AS (

    SELECT {{ hash_sensitive_columns('marketo_activity_sfdc_activity_source') }}
    FROM {{ ref('marketo_activity_sfdc_activity_source') }}

)

SELECT *
FROM source