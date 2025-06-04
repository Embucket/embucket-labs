WITH labels AS (

  SELECT *
  FROM {{ ref('gitlab_ops_labels') }}

), projects AS (

  SELECT 
    project_id,
    visibility_level,
    namespace_id
  FROM {{ ref('gitlab_ops_projects') }}

), joined AS (

    SELECT
      label_id,

    CASE
      WHEN projects.visibility_level != 'public' --AND namespace_id NOT IN (SELECT * FROM internal_namespaces) 
        THEN 'content masked'
      ELSE label_title
    END                                          AS masked_label_title,

    LENGTH(label_title)                          AS title_length,
    color,
    labels.project_id,
    group_id,
    template,
    label_type,
    created_at                                   AS label_created_at,
    updated_at                                   AS label_updated_at

    FROM labels
      LEFT JOIN projects
        ON labels.project_id = projects.project_id

)

SELECT *
FROM joined

