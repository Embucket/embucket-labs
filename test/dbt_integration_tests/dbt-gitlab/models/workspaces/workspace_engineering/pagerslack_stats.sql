WITH sheetload AS (

  SELECT *
  FROM {{ ref('sheetload_pagerslack_stats_source') }}

)

SELECT
  attempts,
  weekend_pinged,
  unavailable,
  incident_url,
  reported_by,
  reported_at,
  time_to_response,
  time_at_response,
  1                                                           AS escalations,
  CASE
    WHEN DATE_PART('hour', time_at_response::TIMESTAMP) > 0
      AND DATE_PART('hour', time_at_response::TIMESTAMP) < 8
      THEN 'APAC'
    WHEN DATE_PART('hour', time_at_response::TIMESTAMP) >= 8
      AND DATE_PART('hour', time_at_response::TIMESTAMP) <= 16
      THEN 'EMEA'
    ELSE 'AMER'
  END                                                         AS timezone,
  time_to_response::FLOAT / 60000                             AS minutes_to_response,
  IFF(unavailable::BOOLEAN = TRUE, 'Response', 'No response')
    AS response_type_copy,
  IFF(unavailable::BOOLEAN = TRUE, 'Escalated', 'Bot')
    AS response_type
FROM
  sheetload
