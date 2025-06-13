{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','csat_survey_response') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR               AS gsid,
    _fivetran_deleted::BOOLEAN  AS _fivetran_deleted,
    email::VARCHAR              AS email,
    survey_id::VARCHAR          AS survey_id,
    created_at::TIMESTAMP       AS created_at,
    csatpercent::VARCHAR        AS csatpercent,
    modified_by::VARCHAR        AS modified_by,
    name::VARCHAR               AS name,
    participant_id::VARCHAR     AS participant_id,
    modified_at::TIMESTAMP      AS modified_at,
    csatscore::NUMBER           AS csatscore,
    score_type::VARCHAR         AS score_type,
    deleted::BOOLEAN            AS deleted,
    company_person_id::VARCHAR  AS company_person_id,
    comment::VARCHAR            AS comment,
    responded_date::TIMESTAMP   AS responded_date,
    created_by::VARCHAR         AS created_by,
    company_id::VARCHAR         AS company_id,
    role::VARCHAR               AS role,
    person_id::VARCHAR          AS person_id,
    question_id::VARCHAR        AS question_id,
    user_answer_id::VARCHAR     AS user_answer_id,
    language_id::VARCHAR        AS language_id,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
