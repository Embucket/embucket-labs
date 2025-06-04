{{ config(
    materialized='table'
) }}

WITH marketo_activity_send_email_source AS (

  SELECT
    'Marketo'                      AS source_system,
    'Send'                         AS email_activity_type,
    lead_id                        AS dim_marketo_person_id,
    marketo_activity_send_email_id AS email_activity_id,
    activity_date,
    campaign_id,
    primary_attribute_value        AS email_name,
    primary_attribute_value_id     AS marketo_email_id
  FROM {{ ref('marketo_activity_send_email_source') }}
  WHERE email_name IS NOT NULL
    AND activity_date IS NOT NULL
),

marketo_activity_open_email_source AS (
  SELECT
    'Marketo'                      AS source_system,
    'Open'                         AS email_activity_type,
    lead_id                        AS dim_marketo_person_id,
    marketo_activity_open_email_id AS email_activity_id,
    activity_date,
    campaign_id,
    primary_attribute_value        AS email_name,
    primary_attribute_value_id     AS marketo_email_id
  FROM {{ ref('marketo_activity_open_email_source') }}
  WHERE email_name IS NOT NULL
    AND activity_date IS NOT NULL
),

marketo_activity_click_email_source AS (
  SELECT
    'Marketo'                       AS source_system,
    'Click'                         AS email_activity_type,
    lead_id                         AS dim_marketo_person_id,
    marketo_activity_click_email_id AS email_activity_id,
    activity_date,
    campaign_id,
    primary_attribute_value         AS email_name,
    primary_attribute_value_id      AS marketo_email_id
  FROM {{ ref('marketo_activity_click_email_source') }}
  WHERE email_name IS NOT NULL
    AND activity_date IS NOT NULL
),

marketo_activity_email_bounced_source AS (
  SELECT
    'Marketo'                         AS source_system,
    'Bounce'                          AS email_activity_type,
    lead_id                           AS dim_marketo_person_id,
    marketo_activity_email_bounced_id AS email_activity_id,
    activity_date,
    campaign_id,
    primary_attribute_value           AS email_name,
    primary_attribute_value_id        AS marketo_email_id
  FROM {{ ref('marketo_activity_email_bounced_source') }}
  WHERE email_name IS NOT NULL
    AND activity_date IS NOT NULL
),

marketo_activity_email_bounced_soft_source AS (
  SELECT
    'Marketo'                              AS source_system,
    'Soft Bounce'                          AS email_activity_type,
    lead_id                                AS dim_marketo_person_id,
    marketo_activity_email_bounced_soft_id AS email_activity_id,
    activity_date,
    campaign_id,
    primary_attribute_value                AS email_name,
    primary_attribute_value_id             AS marketo_email_id
  FROM {{ ref('marketo_activity_email_bounced_soft_source') }}
  WHERE email_name IS NOT NULL
    AND activity_date IS NOT NULL
),

marketo_activity_unsubscribe_email_source AS (
  SELECT
    'Marketo'                             AS source_system,
    'Unsubscribe'                         AS email_activity_type,
    lead_id                               AS dim_marketo_person_id,
    marketo_activity_unsubscribe_email_id AS email_activity_id,
    activity_date,
    campaign_id,
    primary_attribute_value               AS email_name,
    primary_attribute_value_id            AS marketo_email_id
  FROM {{ ref('marketo_activity_unsubscribe_email_source') }}
  WHERE email_name IS NOT NULL
    AND activity_date IS NOT NULL
)
,

gainsight_sends AS (
  SELECT
    'Gainsight'        AS source_system,
    'Send'             AS email_activity_type,
    id                 AS email_activity_id,
    person_id          AS gainsight_person_id,
    triggered_on::DATE AS activity_date,
    batch_name,
    template_id,
    template_name
  FROM {{ ref('email_logs') }}
  WHERE sent = 1
),

gainsight_opens AS (
  SELECT
    'Gainsight'     AS source_system,
    'Open'          AS email_activity_type,
    id              AS email_activity_id,
    person_id       AS gainsight_person_id,
    opened_on::DATE AS activity_date,
    batch_name,
    template_id,
    template_name
  FROM {{ ref('email_logs') }}
  WHERE opened = 1
),

gainsight_clicks AS (
  SELECT
    'Gainsight'      AS source_system,
    'Click'          AS email_activity_type,
    id               AS email_activity_id,
    person_id        AS gainsight_person_id,
    clicked_on::DATE AS activity_date,
    batch_name,
    template_id,
    template_name
  FROM {{ ref('email_logs') }}
  WHERE clicked = 1
),

gainsight_bounces AS (
  SELECT
    'Gainsight'          AS source_system,
    'Bounce'             AS email_activity_type,
    id                   AS email_activity_id,
    person_id            AS gainsight_person_id,
    hard_bounce_on::DATE AS activity_date,
    batch_name,
    template_id,
    template_name
  FROM {{ ref('email_logs') }}
  WHERE hard_bounced = 1
),

gainsight_soft_bounces AS (
  SELECT
    'Gainsight'          AS source_system,
    'Soft Bounce'        AS email_activity_type,
    id                   AS email_activity_id,
    person_id            AS gainsight_person_id,
    soft_bounce_on::DATE AS activity_date,
    batch_name,
    template_id,
    template_name
  FROM {{ ref('email_logs') }}
  WHERE soft_bounced = 1
),

gainsight_unsubscribes AS (
  SELECT
    'Gainsight'           AS source_system,
    'Unsubscribe'         AS email_activity_type,
    id                    AS email_activity_id,
    person_id             AS gainsight_person_id,
    unsubscribed_on::DATE AS activity_date,
    batch_name,
    template_id,
    template_name
  FROM {{ ref('email_logs') }}
  WHERE unsubscribed = 1
)
,

-- ), iterable_emails AS (

--     SELECT

--     FROM 
zuora_emails AS (

  SELECT
    'Zuora'                                         AS source_system,
    'Send'                                          AS email_activity_type,
    zuora_contact_source.contact_id                 AS zuora_person_id,
    zuora_invoice_source.invoice_id                 AS email_activity_id,
    zuora_invoice_source.last_email_sent_date::DATE AS activity_date
  FROM {{ ref('zuora_invoice_source') }}
  LEFT JOIN {{ ref('zuora_contact_source') }}
    ON zuora_invoice_source.account_id = zuora_contact_source.account_id

),

zendesk_emails AS (

  SELECT
    'Zendesk'                                      AS source_system,
    'Send'                                         AS email_activity_type,
    zendesk_tickets_source.requester_id            AS zendesk_person_id,
    zendesk_tickets_source.ticket_id               AS email_activity_id,
    zendesk_tickets_source.ticket_created_at::DATE AS activity_date,
    zendesk_tickets_source.ticket_subject          AS email_subject_line,
    zendesk_users_source.email                     AS from_email
  FROM {{ ref('zendesk_tickets_source') }}
  LEFT JOIN {{ ref('zendesk_users_source') }}
    ON zendesk_tickets_source.assignee_id = zendesk_users_source.user_id
),

-- ), levelup_emails AS (

--     SELECT DISTINCT
--         source
--     FROM PROD.WORKSPACE_PEOPLE.WK_LEVEL_UP_EMAIL_CAPTURES
-- doesn't capture engagement type (open, click, etc.)

-- ), qualtrics_emails AS (

--     SELECT
--         qualtrics_distribution.distribution_id,
--         qualtrics_distribution.mailing_sent_at,

--     FROM PREP.QUALTRICS.QUALTRICS_DISTRIBUTION
--     LEFT JOIN 
--         ON qualtrics_distribution.mailing_list_id=

-- data is aggregated by send, not granular by person/email that engaged

-- ), rally_emails AS (

--     SELECT

--     FROM 
--insufficient privileges

mart_marketo_person AS (
  SELECT DISTINCT
    dim_marketo_person_id,
    email_hash
  FROM {{ ref('mart_marketo_person') }}
),

--marketo union all
marketo_emails_base AS (
  SELECT
    source_system,
    email_activity_type,
    dim_marketo_person_id,
    email_activity_id,
    activity_date,
    campaign_id,
    email_name,
    marketo_email_id
  FROM marketo_activity_send_email_source

  UNION ALL
  SELECT
    source_system,
    email_activity_type,
    dim_marketo_person_id,
    email_activity_id,
    activity_date,
    campaign_id,
    email_name,
    marketo_email_id
  FROM marketo_activity_open_email_source
  UNION ALL
  SELECT
    source_system,
    email_activity_type,
    dim_marketo_person_id,
    email_activity_id,
    activity_date,
    campaign_id,
    email_name,
    marketo_email_id
  FROM marketo_activity_click_email_source
  UNION ALL
  SELECT
    source_system,
    email_activity_type,
    dim_marketo_person_id,
    email_activity_id,
    activity_date,
    campaign_id,
    email_name,
    marketo_email_id
  FROM marketo_activity_email_bounced_source
  UNION ALL
  SELECT
    source_system,
    email_activity_type,
    dim_marketo_person_id,
    email_activity_id,
    activity_date,
    campaign_id,
    email_name,
    marketo_email_id
  FROM marketo_activity_email_bounced_soft_source
  UNION ALL
  SELECT
    source_system,
    email_activity_type,
    dim_marketo_person_id,
    email_activity_id,
    activity_date,
    campaign_id,
    email_name,
    marketo_email_id
  FROM marketo_activity_unsubscribe_email_source
),

marketo_emails AS (

  SELECT
    marketo_emails_base.*,
    mart_marketo_person.email_hash
  FROM marketo_emails_base
  LEFT JOIN mart_marketo_person
    ON marketo_emails_base.dim_marketo_person_id = mart_marketo_person.dim_marketo_person_id

),

gainsight_emails AS (
  SELECT * FROM gainsight_sends
  UNION ALL
  SELECT * FROM gainsight_opens
  UNION ALL
  SELECT * FROM gainsight_clicks
  UNION ALL
  SELECT * FROM gainsight_bounces
  UNION ALL
  SELECT * FROM gainsight_soft_bounces
  UNION ALL
  SELECT * FROM gainsight_unsubscribes
),

-- All our CTEs up to marketo_emails, gainsight_emails, zuora_emails, and zendesk_emails are up until here 

combined_emails AS (
  -- Handling Marketo separately since it's the largest
  WITH marketo_base AS (
    SELECT DISTINCT
      source_system,
      email_activity_type,
      activity_date
    FROM marketo_emails
  ),

  -- Combining all other smaller sources below
  other_sources AS (
    SELECT DISTINCT
      source_system,
      email_activity_type,
      activity_date
    FROM (
      SELECT
        source_system,
        email_activity_type,
        activity_date
      FROM gainsight_emails
      UNION ALL
      SELECT
        source_system,
        email_activity_type,
        activity_date
      FROM zuora_emails
      UNION ALL
      SELECT
        source_system,
        email_activity_type,
        activity_date
      FROM zendesk_emails
    -- UNION ALL
    -- SELECT
    --     source_system,
    --     email_activity_type,
    --     activity_date
    -- FROM levelup_emails
    -- UNION ALL
    -- SELECT
    --     source_system,
    --     email_activity_type,
    --     activity_date
    -- FROM qualtrics_emails
    -- UNION ALL
    -- SELECT
    --     source_system,
    --     email_activity_type,
    --     activity_date
    -- FROM rally_emails
    )
  )

  SELECT * FROM marketo_base
  UNION ALL
  SELECT * FROM other_sources
),

final AS (
  SELECT
    combined_emails.source_system,
    combined_emails.email_activity_type,
    combined_emails.activity_date,
    marketo_emails.dim_marketo_person_id,
    marketo_emails.email_activity_id   AS marketo_email_activity_id,
    gainsight_emails.gainsight_person_id,
    gainsight_emails.email_activity_id AS gainsight_email_activity_id,
    zuora_emails.zuora_person_id,
    zuora_emails.email_activity_id     AS zuora_email_activity_id,
    zendesk_emails.zendesk_person_id,
    zendesk_emails.email_activity_id   AS zendesk_email_activity_id,
    marketo_emails.campaign_id         AS marketo_campaign_id,
    marketo_emails.email_name          AS marketo_email_name,
    marketo_emails.marketo_email_id,
    gainsight_emails.batch_name        AS gainsight_batch_name,
    gainsight_emails.template_id       AS gainsight_template_id,
    gainsight_emails.template_name     AS gainsight_template_name,
    zendesk_emails.email_subject_line  AS zendesk_email_subject_line,
    zendesk_emails.from_email          AS zendesk_from_email
  FROM combined_emails
  LEFT JOIN marketo_emails
    ON combined_emails.source_system = marketo_emails.source_system
      AND combined_emails.email_activity_type = marketo_emails.email_activity_type
      AND combined_emails.activity_date = marketo_emails.activity_date
  LEFT JOIN gainsight_emails
    ON combined_emails.source_system = gainsight_emails.source_system
      AND combined_emails.email_activity_type = gainsight_emails.email_activity_type
      AND combined_emails.activity_date = gainsight_emails.activity_date
  LEFT JOIN zuora_emails
    ON combined_emails.source_system = zuora_emails.source_system
      AND combined_emails.email_activity_type = zuora_emails.email_activity_type
      AND combined_emails.activity_date = zuora_emails.activity_date
  LEFT JOIN zendesk_emails
    ON combined_emails.source_system = zendesk_emails.source_system
      AND combined_emails.email_activity_type = zendesk_emails.email_activity_type
      AND combined_emails.activity_date = zendesk_emails.activity_date
)

SELECT *
FROM final
/*
{{ simple_cte([
    ('marketo_activity_send_email_source','marketo_activity_send_email_source'),
    ('marketo_activity_open_email_source','marketo_activity_open_email_source'),
    ('marketo_activity_click_email_source', 'marketo_activity_click_email_source'),
    ('marketo_activity_email_bounced_source', 'marketo_activity_email_bounced_source'),
    ('marketo_activity_email_bounced_soft_source', 'marketo_activity_email_bounced_soft_source'),
    ('marketo_activity_unsubscribe_email_source', 'marketo_activity_unsubscribe_email_source'),
    ('mart_marketo_person', 'mart_marketo_person'),
    ('email_logs', 'email_logs'),
    ('zuora_invoice_source', 'zuora_invoice_source'),
    ('zuora_contact_source', 'zuora_contact_source'),
    ('zendesk_tickets_source', 'zendesk_tickets_source'),
    ('zendesk_users_source', 'zendesk_users_source')
]) }}

, marketo_emails_base AS (

    SELECT
        'Marketo' AS source_system,
        'Send' AS email_activity_type,
        lead_id AS dim_marketo_person_id,
        marketo_activity_send_email_id AS email_activity_id,
        activity_date,
        campaign_id,
        primary_attribute_value AS email_name,
        primary_attribute_value_id AS marketo_email_id
    FROM marketo_activity_send_email_source
    UNION ALL
    SELECT
        'Marketo' AS source_system,
        'Open' AS email_activity_type,
        lead_id AS dim_marketo_person_id,
        marketo_activity_open_email_id AS email_activity_id,
        activity_date,
        campaign_id,
        primary_attribute_value AS email_name,
        primary_attribute_value_id AS marketo_email_id
    FROM marketo_activity_open_email_source
    UNION ALL
    SELECT
        'Marketo' AS source_system,
        'Click' AS email_activity_type,
        lead_id AS dim_marketo_person_id,
        marketo_activity_click_email_id AS email_activity_id,
        activity_date,
        campaign_id,
        primary_attribute_value AS email_name,
        primary_attribute_value_id AS marketo_email_id
    FROM marketo_activity_click_email_source
    UNION ALL
    SELECT
        'Marketo' AS source_system,
        'Bounce' AS email_activity_type,
        lead_id AS dim_marketo_person_id,
        marketo_activity_email_bounced_id AS email_activity_id,
        activity_date,
        campaign_id,
        primary_attribute_value AS email_name,
        primary_attribute_value_id AS marketo_email_id
    FROM marketo_activity_email_bounced_source
    UNION ALL
    SELECT
        'Marketo' AS source_system,
        'Soft Bounce' AS email_activity_type,
        lead_id AS dim_marketo_person_id,
        marketo_activity_email_bounced_soft_id AS email_activity_id,
        activity_date,
        campaign_id,
        primary_attribute_value AS email_name,
        primary_attribute_value_id AS marketo_email_id
    FROM marketo_activity_email_bounced_soft_source
    UNION ALL
    SELECT
        'Marketo' AS source_system,
        'Unsubscribe' AS email_activity_type,
        lead_id AS dim_marketo_person_id,
        marketo_activity_unsubscribe_email_id AS email_activity_id,
        activity_date,
        campaign_id,
        primary_attribute_value AS email_name,
        primary_attribute_value_id AS marketo_email_id
    FROM marketo_activity_unsubscribe_email_source

), marketo_emails AS (

    SELECT
        marketo_emails_base.*,
        mart_marketo_person.email_hash
    FROM marketo_emails_base
    LEFT JOIN mart_marketo_person
        ON marketo_emails_base.dim_marketo_person_id=mart_marketo_person.dim_marketo_person_id

), gainsight_emails AS (

    SELECT
        'Gainsight' AS source_system,
        'Send' AS email_activity_type,
        id AS email_activity_id,
        person_id AS gainsight_person_id,
        triggered_on::DATE AS activity_date,
        batch_name,
        template_id,
        template_name
    FROM email_logs
    WHERE sent = 1
    UNION ALL
    SELECT
        'Gainsight' AS source_system,
        'Open' AS email_activity_type,
        id AS email_activity_id,
        person_id AS gainsight_person_id,
        opened_on::DATE AS activity_date,
        batch_name,
        template_id,
        template_name
    FROM email_logs
    WHERE opened = 1
    UNION ALL
    SELECT
        'Gainsight' AS source_system,
        'Click' AS email_activity_type,
        id AS email_activity_id,
        person_id AS gainsight_person_id,
        clicked_on::DATE AS activity_date,
        batch_name,
        template_id,
        template_name
    FROM email_logs
    WHERE clicked = 1
    UNION ALL
    SELECT
        'Gainsight' AS source_system,
        'Bounce' AS email_activity_type,
        id AS email_activity_id,
        person_id AS gainsight_person_id,
        hard_bounce_on::DATE AS activity_date,
        batch_name,
        template_id,
        template_name
    FROM email_logs
    WHERE hard_bounced = 1
    UNION ALL
    SELECT
        'Gainsight' AS source_system,
        'Soft Bounce' AS email_activity_type,
        id AS email_activity_id,
        person_id AS gainsight_person_id,
        soft_bounce_on::DATE AS activity_date,
        batch_name,
        template_id,
        template_name
    FROM email_logs
    WHERE soft_bounced = 1
    UNION ALL
    SELECT
        'Gainsight' AS source_system,
        'Unsubscribe' AS email_activity_type,
        id AS email_activity_id,
        person_id AS gainsight_person_id,
        unsubscribed_on::DATE AS activity_date,
        batch_name,
        template_id,
        template_name
    FROM email_logs
    WHERE unsubscribed = 1

-- ), iterable_emails AS (

--     SELECT

--     FROM

), zuora_emails AS (

    SELECT
        'Zuora' AS source_system,
        'Send' AS email_activity_type,
        zuora_contact_source.contact_id AS zuora_person_id,
        zuora_invoice_source.invoice_id AS email_activity_id,
        zuora_invoice_source.last_email_sent_date::DATE AS activity_date
    FROM zuora_invoice_source
    LEFT JOIN zuora_contact_source
        ON zuora_invoice_source.account_id=zuora_contact_source.account_id

), zendesk_emails AS (

    SELECT
        'Zendesk' AS source_system,
        'Send' AS email_activity_type,
        zendesk_tickets_source.requester_id AS zendesk_person_id,
        zendesk_tickets_source.ticket_id AS email_activity_id,
        zendesk_tickets_source.ticket_created_at::DATE AS activity_date,
        zendesk_tickets_source.ticket_subject AS email_subject_line,
        zendesk_users_source.email AS from_email
    FROM zendesk_tickets_source
    LEFT JOIN zendesk_users_source
        ON zendesk_tickets_source.assignee_id=zendesk_users_source.user_id

-- ), levelup_emails AS (

--     SELECT DISTINCT
--         source
--     FROM PROD.WORKSPACE_PEOPLE.WK_LEVEL_UP_EMAIL_CAPTURES
    -- doesn't capture engagement type (open, click, etc.)

-- ), qualtrics_emails AS (

--     SELECT
--         qualtrics_distribution.distribution_id,
--         qualtrics_distribution.mailing_sent_at,

--     FROM PREP.QUALTRICS.QUALTRICS_DISTRIBUTION
--     LEFT JOIN
--         ON qualtrics_distribution.mailing_list_id=

-- data is aggregated by send, not granular by person/email that engaged

-- ), rally_emails AS (

--     SELECT

--     FROM
--insufficient privileges

), combined_emails AS (

    SELECT
        source_system,
        email_activity_type,
        activity_date
    FROM marketo_emails
    UNION ALL
    SELECT
        source_system,
        email_activity_type,
        activity_date
    FROM gainsight_emails
    UNION ALL
    SELECT
        source_system,
        email_activity_type,
        activity_date
    FROM zuora_emails
    UNION ALL
    SELECT
        source_system,
        email_activity_type,
        activity_date
    FROM zendesk_emails
    -- UNION ALL
    -- SELECT
    --     source_system,
    --     email_activity_type,
    --     activity_date
    -- FROM levelup_emails
    -- UNION ALL
    -- SELECT
    --     source_system,
    --     email_activity_type,
    --     activity_date
    -- FROM qualtrics_emails
    -- UNION ALL
    -- SELECT
    --     source_system,
    --     email_activity_type,
    --     activity_date
    -- FROM rally_emails

), final AS (

    SELECT
        combined_emails.source_system,
        combined_emails.email_activity_type,
        combined_emails.activity_date,
        marketo_emails.dim_marketo_person_id,
        marketo_emails.email_activity_id AS marketo_email_activity_id,
        gainsight_emails.gainsight_person_id,
        gainsight_emails.email_activity_id AS gainsight_email_activity_id,
        zuora_emails.zuora_person_id,
        zuora_emails.email_activity_id AS zuora_email_activity_id,
        zendesk_emails.zendesk_person_id,
        zendesk_emails.email_activity_id AS zendesk_email_activity_id,
        marketo_emails.campaign_id AS marketo_campaign_id,
        marketo_emails.email_name AS marketo_email_name,
        marketo_emails.marketo_email_id,
        gainsight_emails.batch_name AS gainsight_batch_name,
        gainsight_emails.template_id AS gainsight_template_id,
        gainsight_emails.template_name AS gainsight_template_name,
        zendesk_emails.email_subject_line AS zendesk_email_subject_line,
        zendesk_emails.from_email AS zendesk_from_email
    FROM combined_emails
    LEFT JOIN marketo_emails
        ON combined_emails.source_system=marketo_emails.source_system
            AND combined_emails.email_activity_type=marketo_emails.email_activity_type
            AND combined_emails.activity_date=marketo_emails.activity_date
    LEFT JOIN gainsight_emails
        ON combined_emails.source_system=gainsight_emails.source_system
            AND combined_emails.email_activity_type=gainsight_emails.email_activity_type
            AND combined_emails.activity_date=gainsight_emails.activity_date
    LEFT JOIN zuora_emails
        ON combined_emails.source_system=zuora_emails.source_system
            AND combined_emails.email_activity_type=zuora_emails.email_activity_type
            AND combined_emails.activity_date=zuora_emails.activity_date
    LEFT JOIN zendesk_emails
        ON combined_emails.source_system=zendesk_emails.source_system
            AND combined_emails.email_activity_type=zendesk_emails.email_activity_type
            AND combined_emails.activity_date=zendesk_emails.activity_date

)

SELECT *
FROM final
*/
