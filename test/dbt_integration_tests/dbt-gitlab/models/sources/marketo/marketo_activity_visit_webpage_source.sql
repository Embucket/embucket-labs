WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_visit_webpage') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS marketo_activity_visit_webpage_id,
      lead_id::NUMBER                           AS lead_id,
      activity_date::TIMESTAMP_TZ               AS activity_date,
      activity_type_id::NUMBER                  AS activity_type_id,
      campaign_id::NUMBER                       AS campaign_id,
      primary_attribute_value_id::NUMBER        AS primary_attribute_value_id,
      primary_attribute_value::TEXT             AS primary_attribute_value,
      webpage_url::TEXT                         AS webpage_url,
      search_engine::TEXT                       AS search_engine,
      client_ip_address::TEXT                   AS client_ip_address,
      user_agent::TEXT                          AS user_agent,
      query_parameters::TEXT                    AS query_parameters,
      referrer_url::TEXT                        AS referrer_url,
      search_query::TEXT                        AS search_query

    FROM source

)

SELECT *
FROM renamed
