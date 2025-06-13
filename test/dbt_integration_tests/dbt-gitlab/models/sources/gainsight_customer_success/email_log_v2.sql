{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','email_log_v_2') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR                               AS gs_id,
    _fivetran_deleted::BOOLEAN                  AS fivetran_deleted,
    _fivetran_synced::TIMESTAMP                 AS fivetran_synced,
    rejected_date::TIMESTAMP                    AS rejected_date,
    gs_user_id::VARCHAR                         AS gs_user_id,
    reply_to_name::VARCHAR                      AS reply_to_name,
    email_group_id::VARCHAR                     AS email_group_id,
    sent_date::TIMESTAMP                        AS sent_date,
    link_clicked_count::NUMBER                  AS link_clicked_count,
    name::VARCHAR                               AS name,
    is_sent::VARCHAR                            AS is_sent,
    first_open_date::TIMESTAMP                  AS first_open_date,
    email_content_id::VARCHAR                   AS email_content_id,
    external_relationship_person_id::VARCHAR    AS external_relationship_person_id,
    is_opened::VARCHAR                          AS is_opened,
    external_company_person_id::VARCHAR         AS external_company_person_id,
    external_user_id::VARCHAR                   AS external_user_id,
    bounce_type::VARCHAR                        AS bounce_type,
    gs_person_id::VARCHAR                       AS gs_person_id,
    gs_person_type::VARCHAR                     AS gs_person_type,
    page_id::VARCHAR                            AS page_id,
    email_id::VARCHAR                           AS email_id,
    link_clicked_date::TIMESTAMP                AS link_clicked_date,
    email_template_name::VARCHAR                AS email_template_name,
    request_id::VARCHAR                         AS request_id,
    is_bounced::VARCHAR                         AS is_bounced,
    spam_marked_date::TIMESTAMP                 AS spam_marked_date,
    category_ids::VARCHAR                       AS category_ids,
    latest_open_date::TIMESTAMP                 AS latest_open_date,
    is_rejected::VARCHAR                        AS is_rejected,
    created_date::TIMESTAMP                     AS created_date,
    recipient_gs_id::VARCHAR                    AS recipient_gs_id,
    is_unsubscribed::VARCHAR                    AS is_unsubscribed,
    is_spam::VARCHAR                            AS is_spam,
    executed_date::TIMESTAMP                    AS executed_date,
    parent_recipient_gs_id::VARCHAR             AS parent_recipient_gs_id,
    record_external_id::NUMBER                  AS record_external_id,
    source_id::VARCHAR                          AS source_id,
    email_template_id::VARCHAR                  AS email_template_id,
    sent_by::VARCHAR                            AS sent_by,
    modified_date::TIMESTAMP                    AS modified_date,
    source::VARCHAR                             AS source,
    gs_company_person_id::VARCHAR               AS gs_company_person_id,
    email_template_version_name::VARCHAR        AS email_template_version_name,
    from_name::VARCHAR                          AS from_name,
    opened_count::NUMBER                        AS opened_count,
    lower_case_email_id::VARCHAR                AS lower_case_email_id,
    record_id::VARCHAR                          AS record_id,
    reply_to_email_id::VARCHAR                  AS reply_to_email_id,
    link_clicked_json::VARIANT                  AS link_clicked_json,
    source_name::VARCHAR                        AS source_name,
    bounced_reason::VARCHAR                     AS bounced_reason,
    gs_company_id::VARCHAR                      AS gs_company_id,
    external_company_id::VARCHAR                AS external_company_id,
    bounce_ddate::TIMESTAMP                     AS bounce_ddate,
    email_template_version_id::VARCHAR          AS email_template_version_id,
    unsubscribed_date::TIMESTAMP                AS unsubscribed_date,
    from_email_id::VARCHAR                      AS from_email_id,
    is_group_email::VARCHAR                     AS is_group_email,
    address_type::VARCHAR                       AS address_type
  FROM source
)

SELECT *
FROM renamed