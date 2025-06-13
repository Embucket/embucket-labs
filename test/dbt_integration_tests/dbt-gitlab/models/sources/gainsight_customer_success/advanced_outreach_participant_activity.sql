{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','advanced_outreach_participant_activity') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR                    AS gsid,
    _fivetran_deleted::BOOLEAN       AS _fivetran_deleted,
    previous_step_id::VARCHAR        AS previous_step_id,
    created_at::TIMESTAMP            AS created_at,
    previous_step_name::VARCHAR      AS previous_step_name,
    current_step_id::VARCHAR         AS current_step_id,
    participant_id::VARCHAR          AS participant_id,
    modified_at::TIMESTAMP           AS modified_at,
    participantactivity_id::VARCHAR  AS participantactivity_id,
    current_step_name::VARCHAR       AS current_step_name,
    advancedoutreach_id::VARCHAR     AS advancedoutreach_id,
    gs_participant_id::VARCHAR       AS gs_participant_id,
    gs_advanced_outreach_id::VARCHAR AS gs_advanced_outreach_id,
    _fivetran_synced::TIMESTAMP      AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed