{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','activity_attendee') }}
),

renamed AS (

  SELECT
    activity_id::VARCHAR               AS activity_id,
    _fivetran_deleted::BOOLEAN         AS _fivetran_deleted,
    attendee_type::VARCHAR             AS attendee_type,
    external_attendee_sfdc_id::VARCHAR AS external_attendee_sfdc_id,
    internal_attendee_sfdc_id::VARCHAR AS internal_attendee_sfdc_id,
    external_attendee_gs_id::VARCHAR   AS external_attendee_gs_id,
    internal_attendee_gs_id::VARCHAR   AS internal_attendee_gs_id,
    _fivetran_synced::TIMESTAMP        AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
