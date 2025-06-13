{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','nps_survey_response') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR               AS gsid,
    _fivetran_deleted::BOOLEAN  AS _fivetran_deleted,
    survey_id::VARCHAR          AS survey_id,
    created_at::VARCHAR         AS created_at,
    user_email::VARCHAR         AS user_email,
    modified_by::VARCHAR        AS modified_by,
    participant_id::VARCHAR     AS participant_id,
    true_nps::VARCHAR           AS true_nps,
    modified_at::VARCHAR        AS modified_at,
    score_type::VARCHAR         AS score_type,
    deleted::VARCHAR            AS deleted,
    company_person_id::VARCHAR  AS company_person_id,
    comment::VARCHAR            AS comment,
    responded_date::VARCHAR     AS responded_date,
    user_name::VARCHAR          AS user_name,
    created_by::VARCHAR         AS created_by,
    company_id::VARCHAR         AS company_id,
    sentiment::VARCHAR          AS sentiment,
    npsscore::VARCHAR           AS npsscore,
    person_id::VARCHAR          AS person_id,
    user_role::VARCHAR          AS user_role,
    language_id::VARCHAR        AS language_id,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
