{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','email_logs') }}
),

renamed AS (

  SELECT
    id::VARCHAR                 AS id,
    _fivetran_deleted::BOOLEAN  AS _fivetran_deleted,
    gs_user_id::VARCHAR         AS gs_user_id,
    clicked::NUMBER             AS clicked,
    rejected_on::TIMESTAMP      AS rejected_on,
    opened_on::TIMESTAMP        AS opened_on,
    soft_bounced::NUMBER        AS soft_bounced,
    person_type::VARCHAR        AS person_type,
    rejected::NUMBER            AS rejected,
    company_name::VARCHAR       AS company_name,
    clicked_on::TIMESTAMP       AS clicked_on,
    sent::NUMBER                AS sent,
    opened::NUMBER              AS opened,
    batch_id::VARCHAR           AS batch_id,
    triggered_on::TIMESTAMP     AS triggered_on,
    email_address::VARCHAR      AS email_address,
    hard_bounced::NUMBER        AS hard_bounced,
    marked_spam::NUMBER         AS marked_spam,
    person_id::VARCHAR          AS person_id,
    clicked_url::VARCHAR        AS clicked_url,
    soft_bounce_on::VARCHAR     AS soft_bounce_on,
    delayed_on::VARCHAR         AS delayed_on,
    parent_gsid::VARCHAR        AS parent_gsid,
    marked_spam_on::VARCHAR     AS marked_spam_on,
    triggered_date::TIMESTAMP   AS triggered_date,
    batch_name::VARCHAR         AS batch_name,
    open_count::NUMBER          AS open_count,
    clicked_ip::VARCHAR         AS clicked_ip,
    template_name::VARCHAR      AS template_name,
    opened_ip::VARCHAR          AS opened_ip,
    template_id::VARCHAR        AS template_id,
    unsubscribed::NUMBER        AS unsubscribed,
    hard_bounce_on::VARCHAR     AS hard_bounce_on,
    person_name::VARCHAR        AS person_name,
    user_name::VARCHAR          AS user_name,
    delayed::NUMBER             AS delayed,
    company_id::VARCHAR         AS company_id,
    variant_name::VARCHAR       AS variant_name,
    unsubscribed_on::VARCHAR    AS unsubscribed_on,
    use_case::VARCHAR           AS use_case,
    variant_id::VARCHAR         AS variant_id,
    click_count::NUMBER         AS click_count,
    event_message::VARCHAR      AS event_message,
    address_type::VARCHAR       AS address_type,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
