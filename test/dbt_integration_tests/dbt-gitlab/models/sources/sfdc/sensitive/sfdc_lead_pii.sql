WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_lead_source') }}

), sfdc_lead_pii AS (

    SELECT
        lead_id,
        {{ nohash_sensitive_columns('sfdc_lead_source','lead_email') }}
    FROM source

)

SELECT *
FROM sfdc_lead_pii
