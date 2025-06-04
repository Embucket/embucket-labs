    
WITH all_labels AS (

  SELECT

    id::NUMBER                                AS label_id,
    color::VARCHAR                            AS color,
    project_id::NUMBER                        AS project_id,
    group_id::NUMBER                          AS group_id,
    template::VARCHAR                         AS template,
    type::VARCHAR                             AS label_type,
    created_at::TIMESTAMP                     AS created_at,
    updated_at::TIMESTAMP                     AS updated_at

  FROM {{ ref('gitlab_dotcom_labels_dedupe_source') }}
  
),

internal_labels AS (

  SELECT
    
    id::NUMBER                                AS internal_label_id,
    title::VARCHAR                            AS internal_label_title
  
  FROM {{ ref('gitlab_dotcom_labels_internal_only_dedupe_source') }}

),

joined AS (

  SELECT

    all_labels.label_id                       AS label_id,
    internal_labels.internal_label_title      AS label_title,
    all_labels.color                          AS color,
    all_labels.project_id                     AS project_id,
    all_labels.group_id                       AS group_id,
    all_labels.template                       AS template,
    all_labels.label_type                     AS label_type,
    all_labels.created_at                     AS created_at,
    all_labels.updated_at                     AS updated_at

  FROM all_labels
    LEFT JOIN internal_labels
      ON all_labels.label_id = internal_labels.internal_label_id 
)

SELECT *
FROM joined
