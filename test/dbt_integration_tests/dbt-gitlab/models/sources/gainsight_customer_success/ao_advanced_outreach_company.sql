{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','ao_advanced_outreach_company') }}
),

renamed AS (

  SELECT
    _fivetran_id::VARCHAR                   AS _fivetran_id,
    _fivetran_deleted::BOOLEAN              AS _fivetran_deleted,
    advanced_outreach_gsid::VARCHAR         AS advanced_outreach_gsid,
    advanced_outreach_name::VARCHAR         AS advanced_outreach_name,
    survey_id::VARCHAR                      AS survey_id,
    advanced_outreach_id::VARCHAR           AS advanced_outreach_id,
    survey_name::VARCHAR                    AS survey_name,
    advanced_outreach_status::VARCHAR       AS advanced_outreach_status,
    created_at::TIMESTAMP                   AS created_at,
    modified_by::VARCHAR                    AS modified_by,
    modified_at::TIMESTAMP                  AS modified_at,
    advanced_outreach_start_date::TIMESTAMP AS advanced_outreach_start_date,
    default_account_id::VARCHAR             AS default_account_id,
    advanced_outreach_type::VARCHAR         AS advanced_outreach_type,
    advanced_outreach_model_name::VARCHAR   AS advanced_outreach_model_name,
    created_by::VARCHAR                     AS created_by,
    _fivetran_synced::TIMESTAMP             AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
