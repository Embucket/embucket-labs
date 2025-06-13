
WITH unioned AS (

  {{ union_tables(
    [
        source('mailgun', 'events_rejected'),
        source('mailgun', 'events_delivered'),
        source('mailgun', 'events_failed'),
        source('mailgun', 'events_opened'),
        source('mailgun', 'events_clicked'),
        source('mailgun', 'events_unsubscribed'),
        source('mailgun', 'events_complained')
    ])
  }}

), extracted AS (

  SELECT try_parse_json(jsontext) AS json_text,
         uploaded_at              AS uploaded_at
    FROM unioned

), formatted AS (

  SELECT VALUE:"domain"::VARCHAR                                    AS domain,
         VALUE:"id"::VARCHAR                                        AS id,
         VALUE:"envelope_sender"::VARCHAR                           AS envelope_sender,
         VALUE:"envelope_targets"::VARCHAR                          AS envelope_targets,
         VALUE:"primary-dkim"::VARCHAR                              AS primary_dkim,
         VALUE:"message_headers_from"::VARCHAR                      AS message_headers_from,
         VALUE:"message_headers_message-id"::VARCHAR                AS message_headers_message_id,
         VALUE:"message_headers_to"::VARCHAR                        AS message_headers_to,
         VALUE:"event"::VARCHAR                                     AS event,
         VALUE:"recipient-domain"::VARCHAR                          AS recipient_domain,
         VALUE:"recipient-provider"::VARCHAR                        AS recipient_provider,
         VALUE:"log-level"::VARCHAR                                 AS log_level,
         VALUE:"recipient"::VARCHAR                                 AS recipient,
         VALUE:"delivery-status_code"::NUMBER                       AS delivery_status_code,
         VALUE:"delivery-status_mx-host"::VARCHAR                   AS delivery_status_mx_host,
         VALUE:"delivery-status_description"::VARCHAR               AS delivery_status_description,
         VALUE:"delivery-status_attempt-no"::NUMBER                 AS delivery_status_attempt_no,
         VALUE:"delivery-status_message"::VARCHAR                   AS delivery_status_message,
         DATEADD('sec', VALUE:"timestamp", '1970-01-01')::TIMESTAMP AS timestamp_updated,
         VALUE:"campaigns"::VARCHAR                                 AS campaigns,
         VALUE:"reason"::VARCHAR                                    AS reason,
         VALUE:"delivery-status_bounce-code"::VARCHAR               AS delivery_status_bounce_code,
         VALUE:"delivery-status_bounce-type"::VARCHAR               AS delivery_status_bounce_type,
         VALUE:"geolocation_region"::VARCHAR                        AS geolocation_region,
         VALUE:"geolocation_city"::VARCHAR                          AS geolocation_city,
         VALUE:"geolocation_timezone"::VARCHAR                      AS geolocation_timezone,
         VALUE:"geolocation_country"::VARCHAR                       AS geolocation_country,
         uploaded_at::TIMESTAMP                                     AS uploaded_at
    FROM extracted,
    LATERAL FLATTEN(INPUT => json_text)

), deduped AS (

  SELECT *
  FROM formatted
  QUALIFY ROW_NUMBER() OVER (PARTITION BY domain, id ORDER BY uploaded_at DESC) = 1

)

  SELECT *
  FROM deduped