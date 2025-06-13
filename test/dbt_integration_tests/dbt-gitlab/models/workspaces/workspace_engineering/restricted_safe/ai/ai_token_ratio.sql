WITH source AS (
  SELECT
    e.behavior_date  AS behavior_date,
    e.event_action   AS event_action,
    e.event_label    AS feature,
    e.event_category AS model,
    e.event_property AS request_id,
    e.event_value    AS token_amount,
    e.contexts
  FROM
    {{ ref ('mart_behavior_structured_event') }} AS e
  WHERE
    (
      e.event_action = 'tokens_per_user_request_prompt'
      OR
      e.event_action = 'tokens_per_user_request_response'
    )
    AND
    e.behavior_date > '2024-01-01'
    AND
    e.app_id = 'gitlab'
)

SELECT
  DATE_TRUNC('day', behavior_date) AS day,
  CASE WHEN model LIKE '%Anthropic%' OR LOWER(model) LIKE '%aigateway%' THEN 'Anthropic'
    ELSE 'Google'
  END                              AS provider,
  model                            AS model,
  feature                          AS feature,
  event_action                     AS prompt_answer,
  SUM(token_amount)                AS tokens_tracked,
  SUM(token_amount) * 4            AS characters_tracked
FROM source
GROUP BY 1, 2, 3, 4, 5
