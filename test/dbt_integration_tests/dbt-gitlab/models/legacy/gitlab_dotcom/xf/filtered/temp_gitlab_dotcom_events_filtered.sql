{{ config(
    materialized = "incremental",
    cluster_by=['target_type', 'event_action_type_id']
) }}

SELECT
  event_action_type,
  event_action_type_id,
  target_type,
  created_at,
  author_id,
  project_id,
  event_id
FROM {{ ref('gitlab_dotcom_events') }}
WHERE created_at IS NOT NULL
  AND created_at >= DATEADD(MONTH, -25, CURRENT_DATE)
  AND (
    (target_type IS NULL AND event_action_type_id = 5) OR
    (target_type = 'DesignManagement::Design' AND event_action_type_id IN (1,2)) OR
    (target_type = 'WikiPage::Meta' AND event_action_type_id IN (1,2)) OR
    (event_action_type = 'pushed')
  )

  {% if is_incremental() %}

    AND created_at > (SELECT MAX(created_at) FROM {{ this }})

  {% endif %}
