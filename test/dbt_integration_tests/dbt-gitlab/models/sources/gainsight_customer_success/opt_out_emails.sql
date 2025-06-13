{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','opt_out_emails') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR                 AS gsid,
    _fivetran_deleted::BOOLEAN    AS _fivetran_deleted,
    category_id::VARCHAR          AS category_id,
    page_id::VARCHAR              AS page_id,
    company_id::VARCHAR           AS company_id,
    entity_name::VARCHAR          AS entity_name,
    person_type::VARCHAR          AS person_type,
    email_address::VARCHAR        AS email_address,
    reason::VARCHAR               AS reason,
    entity_id::VARCHAR            AS entity_id,
    entity_type::VARCHAR          AS entity_type,
    modified_at::TIMESTAMP        AS modified_at,
    person_id::VARCHAR            AS person_id,
    reason_text::VARCHAR          AS reason_text,
    full_name::VARCHAR            AS full_name,
    reason_type::VARCHAR          AS reason_type,
    email_log_ide_7940_f::VARCHAR AS email_log_ide_7940_f,
    _fivetran_synced::TIMESTAMP   AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed