{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','activity_timeline') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR                                       AS gsid,
    _fivetran_deleted::BOOLEAN                          AS _fivetran_deleted,
    last_modified_date::TIMESTAMP                       AS last_modified_date,
    type_name::VARCHAR                                  AS type_name,
    ant_exec_sponsor_present_c::BOOLEAN                 AS ant_exec_sponsor_present_c,
    account_id::VARCHAR                                 AS account_id,
    ant_risk_impact_c::VARCHAR                          AS ant_risk_impact_c,
    ant_risk_reason_c::VARCHAR                          AS ant_risk_reason_c,
    ant_attendee_count_c::VARCHAR                       AS ant_attendee_count_c,
    ant_create_stage_enable_or_expansion_cta_c::BOOLEAN AS ant_create_stage_enable_or_expansion_cta_c,
    reporting_category::VARCHAR                         AS reporting_category,
    ant_requires_triage_c::BOOLEAN                      AS ant_requires_triage_c,
    ant_exec_check_in_c::BOOLEAN                        AS ant_exec_check_in_c,
    created_by::VARCHAR                                 AS created_by,
    activity_date::TIMESTAMP                            AS activity_date,
    milestone_type_name::VARCHAR                        AS milestone_type_name,
    internal_attendees::VARCHAR                         AS internal_attendees,
    ant_executive_escalation_c::BOOLEAN                 AS ant_executive_escalation_c,
    score_details::VARCHAR                              AS score_details,
    context_id::VARCHAR                                 AS context_id,
    created_date::TIMESTAMP                             AS created_date,
    last_modified_by::VARCHAR                           AS last_modified_by,
    gs_created_by_user_id::VARCHAR                      AS gs_created_by_user_id,
    activity_id::VARCHAR                                AS activity_id,
    user_email::VARCHAR                                 AS user_email,
    source::VARCHAR                                     AS source,
    scorecard::VARCHAR                                  AS scorecard,
    notes::VARCHAR                                      AS notes,
    ant_trending_c::VARCHAR                             AS ant_trending_c,
    ant_workshop_topic_c::VARCHAR                       AS ant_workshop_topic_c,
    ant_status_c::VARCHAR                               AS ant_status_c,
    user_name::VARCHAR                                  AS user_name,
    author_id::VARCHAR                                  AS author_id,
    scorecard_measure::VARCHAR                          AS scorecard_measure,
    external_id::VARCHAR                                AS external_id,
    ant_meeting_type_c::VARCHAR                         AS ant_meeting_type_c,
    gs_lastmodified_by_user_id::VARCHAR                 AS gs_lastmodified_by_user_id,
    gs_company_id::VARCHAR                              AS gs_company_id,
    notes_plain_text::VARCHAR                           AS notes_plain_text,
    subject::VARCHAR                                    AS subject,
    ant_product_risk_c::VARCHAR                         AS ant_product_risk_c,
    sf_task_id::VARCHAR                                 AS sf_task_id,
    user_id::VARCHAR                                    AS user_id,
    external_attendees::VARCHAR                         AS external_attendees,
    ant_sentiment_c::VARCHAR                            AS ant_sentiment_c,
    context_name::VARCHAR                               AS context_name,
    _fivetran_synced::TIMESTAMP                         AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
