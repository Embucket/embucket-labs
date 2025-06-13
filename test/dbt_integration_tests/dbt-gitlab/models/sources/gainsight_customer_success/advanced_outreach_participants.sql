{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','advanced_outreach_participants') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR                        AS gsid,
    _fivetran_deleted::BOOLEAN           AS _fivetran_deleted,
    source_type::VARCHAR                 AS source_type,
    created_at::TIMESTAMP                AS created_at,
    participant_source_type::VARCHAR     AS participant_source_type,
    person_type::VARCHAR                 AS person_type,
    gs_account_id::VARCHAR               AS gs_account_id,
    participant_state::VARCHAR           AS participant_state,
    participant_created_by::VARCHAR      AS participant_created_by,
    participant_id::VARCHAR              AS participant_id,
    survey_page_opened::BOOLEAN          AS survey_page_opened,
    context_attribute_boolean_2::BOOLEAN AS context_attribute_boolean_2,
    context_attribute_boolean_1::BOOLEAN AS context_attribute_boolean_1,
    last_email_status::VARCHAR           AS last_email_status,
    survey_response_page_url::VARCHAR    AS survey_response_page_url,
    context_attribute_boolean_3::BOOLEAN AS context_attribute_boolean_3,
    gs_person_id::VARCHAR                AS gs_person_id,
    total_email_sent::NUMBER             AS total_email_sent,
    survey_responded_date::TIMESTAMP     AS survey_responded_date,
    sfdc_account_id::VARCHAR             AS sfdc_account_id,
    survey_send_date::VARCHAR            AS survey_send_date,
    sender_last_name::VARCHAR            AS sender_last_name,
    sender_first_name::VARCHAR           AS sender_first_name,
    sender_email_address::VARCHAR        AS sender_email_address,
    survey_score::VARCHAR                AS survey_score,
    last_email_sent::TIMESTAMP           AS last_email_sent,
    recipient_email_address::VARCHAR     AS recipient_email_address,
    context_id::VARCHAR                  AS context_id,
    sfdc_contact_id::VARCHAR             AS sfdc_contact_id,
    last_name::VARCHAR                   AS last_name,
    survey_page_open_count::NUMBER       AS survey_page_open_count,
    gs_advanced_outreach_id::VARCHAR     AS gs_advanced_outreach_id,
    account_name::VARCHAR                AS account_name,
    advanced_outreach_id::VARCHAR        AS advanced_outreach_id,
    sender_full_name::VARCHAR            AS sender_full_name,
    failure_reasons::VARCHAR             AS failure_reasons,
    total_email_clicked::VARCHAR         AS total_email_clicked,
    gs_source_id::VARCHAR                AS gs_source_id,
    survey_response_status::VARCHAR      AS survey_response_status,
    survey_responded::BOOLEAN            AS survey_responded,
    modified_at::TIMESTAMP               AS modified_at,
    survey_url::VARCHAR                  AS survey_url,
    participant_type::VARCHAR            AS participant_type,
    deleted::BOOLEAN                     AS deleted,
    gs_manager_user_id::VARCHAR          AS gs_manager_user_id,
    participant_source_name::VARCHAR     AS participant_source_name,
    first_name::VARCHAR                  AS first_name,
    context_attribute_integer_3::NUMBER  AS context_attribute_integer_3,
    context_attribute_integer_2::NUMBER  AS context_attribute_integer_2,
    context_attribute_integer_1::NUMBER  AS context_attribute_integer_1,
    survey_response_source::VARCHAR      AS survey_response_source,
    manager_user_id::VARCHAR             AS manager_user_id,
    full_name::VARCHAR                   AS full_name,
    last_email_link_clicked::VARCHAR     AS last_email_link_clicked,
    _fivetran_synced::TIMESTAMP          AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed