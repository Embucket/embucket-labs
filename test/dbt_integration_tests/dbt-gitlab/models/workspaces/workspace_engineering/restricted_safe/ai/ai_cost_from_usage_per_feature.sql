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
),

inter AS (
  SELECT
    DATE_TRUNC('day', source.behavior_date) AS day,
    CASE WHEN model LIKE '%Anthropic%' OR LOWER(model) LIKE '%aigateway%' THEN 'Anthropic'
      ELSE 'Vertex'
    END                                     AS model,

    feature,

    CASE WHEN event_action LIKE '%prompt%' THEN 'prompt/input'
      WHEN event_action LIKE '%response%' THEN 'response/output'
    END                                     AS usage_type,

    SUM(token_amount)                       AS tokens,

    CASE WHEN event_action = 'tokens_per_user_request_prompt' THEN SUM(token_amount) / 1000000 * 8
      WHEN event_action = 'tokens_per_user_request_response' THEN SUM(token_amount) / 1000000 * 24
      ELSE 0
    END                                     AS tracked_cost, -- tracking x unit-cost from Anthropic

    CASE WHEN event_action = 'tokens_per_user_request_prompt' THEN (SUM(token_amount) / 1000000 * 8) * (1 / 0.898) -- increasing unit cost by 1/coverage
      WHEN event_action = 'tokens_per_user_request_response' THEN (SUM(token_amount) / 1000000 * 24) * (1 / 0.54) -- increasing unit-cost by 1/coverage
      ELSE 0
    END                                     AS extrapolated_cost -- assuming same coverage in april than March

  FROM source
  GROUP BY 1, 2, 3, 4, event_action
)

SELECT * FROM inter
WHERE model = 'Anthropic' -- only Anthropic usage matches the bil so far
