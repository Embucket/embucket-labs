WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_contact_source') }}

), sfdc_contact_pii AS (

    SELECT
      contact_id,
      {{ nohash_sensitive_columns('sfdc_contact_source','contact_email') }}
    FROM source

)

SELECT *
FROM sfdc_contact_pii
