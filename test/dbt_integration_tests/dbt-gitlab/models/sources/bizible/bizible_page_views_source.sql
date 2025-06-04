WITH source AS (

    SELECT
      id                            AS page_view_id,
      cookie_id                     AS cookie_id,
      visitor_id                    AS visitor_id,
      session_id                    AS session_id,
      event_date                    AS event_date,
      modified_date                 AS modified_date,
      current_page                  AS current_page,
      current_page_raw              AS current_page_raw,
      ip_address                    AS ip_address,
      city                          AS city,
      region                        AS region,
      country                       AS country,
      type                          AS type,
      user_agent_string             AS user_agent_string,
      client_sequence               AS client_sequence,
      client_random                 AS client_random,
      is_duplicated                 AS is_duplicated,
      is_processed                  AS is_processed,
      referrer_page                 AS referrer_page,
      referrer_page_raw             AS referrer_page_raw,
      page_title                    AS page_title,
      email                         AS email,
      has_user_consent              AS has_user_consent,
      row_key                       AS row_key,
      current_page_key              AS current_page_key,
      referrer_page_key             AS referrer_page_key,
      _created_date                 AS _created_date,
      _modified_date                AS _modified_date,
      _deleted_date                 AS _deleted_date
    FROM {{ source('bizible', 'biz_page_views') }}
 
)

SELECT *
FROM source

