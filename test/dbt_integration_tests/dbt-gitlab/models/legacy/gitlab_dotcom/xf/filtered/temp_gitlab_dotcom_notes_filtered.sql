{{ config(
    materialized = "incremental",
    cluster_by = ['noteable_type']
) }}

SELECT
  note_author_id,
  project_id,
  note_id,
  created_at,
  noteable_type
FROM {{ ref('gitlab_dotcom_notes') }}
WHERE created_at IS NOT NULL
  AND created_at >= DATEADD(MONTH, -25, CURRENT_DATE)
  AND noteable_type IN (
    'Issue',
    'MergeRequest'
  )

  {% if is_incremental() %}

    AND created_at > (SELECT MAX(created_at) FROM {{ this }})

  {% endif %}
