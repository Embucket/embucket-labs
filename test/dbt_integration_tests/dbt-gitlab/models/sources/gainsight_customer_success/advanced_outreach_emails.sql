{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','advanced_outreach_emails') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR                         AS gsid,
    _fivetran_deleted::BOOLEAN            AS _fivetran_deleted,
    rejected_date::TIMESTAMP              AS rejected_date,
    created_at::TIMESTAMP                 AS created_at,
    link_clicked_count::NUMBER            AS link_clicked_count,
    rejected::BOOLEAN                     AS rejected,
    participant_id::VARCHAR               AS participant_id,
    email_service_provider::VARCHAR       AS email_service_provider,
    step_id::VARCHAR                      AS step_id,
    gs_participant_id::VARCHAR            AS gs_participant_id,
    email_subject::VARCHAR                AS email_subject,
    bounce_type::VARCHAR                  AS bounce_type,
    email_id::VARCHAR                     AS email_id,
    email_template_variance_id::VARCHAR   AS email_template_variance_id,
    email_clicked::BOOLEAN                AS email_clicked,
    email_template_name::VARCHAR          AS email_template_name,
    delayed_date::TIMESTAMP               AS delayed_date,
    parent_gs_id::VARCHAR                 AS parent_gs_id,
    email_template_variance_name::VARCHAR AS email_template_variance_name,
    html_body::VARCHAR                    AS html_body,
    page_open_count::NUMBER               AS page_open_count,
    email_opened_date::TIMESTAMP          AS email_opened_date,
    gs_advanced_outreach_id::VARCHAR      AS gs_advanced_outreach_id,
    to_address::VARCHAR                   AS to_address,
    email_log_id::VARCHAR                 AS email_log_id,
    survey_id::VARCHAR                    AS survey_id,
    advanced_outreach_id::VARCHAR         AS advanced_outreach_id,
    email_template_id::VARCHAR            AS email_template_id,
    last_linked_clicked::TIMESTAMP        AS last_linked_clicked,
    spam::BOOLEAN                         AS spam,
    modified_at::TIMESTAMP                AS modified_at,
    from_address::VARCHAR                 AS from_address,
    spam_date::TIMESTAMP                  AS spam_date,
    bounce::BOOLEAN                       AS bounce,
    deleted::BOOLEAN                      AS deleted,
    email_send_time::TIMESTAMP            AS email_send_time,
    unsubscribed::BOOLEAN                 AS unsubscribed,
    delayed::BOOLEAN                      AS delayed,
    external_id::VARCHAR                  AS external_id,
    bounce_date::TIMESTAMP                AS bounce_date,
    undelivered::BOOLEAN                  AS undelivered,
    email_send::BOOLEAN                   AS email_send,
    text_body::VARCHAR                    AS text_body,
    last_email_opened_date::TIMESTAMP     AS last_email_opened_date,
    email_clicked_date::TIMESTAMP         AS email_clicked_date,
    unsubscribed_date::TIMESTAMP          AS unsubscribed_date,
    email_open_count::NUMBER              AS email_open_count,
    email_opened::BOOLEAN                 AS email_opened,
    ip_address::VARCHAR                   AS ip_address,
    address_type::VARCHAR                 AS address_type,
    _fivetran_synced::TIMESTAMP           AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed