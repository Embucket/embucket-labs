WITH source AS (

  SELECT *
  FROM EMBUCKET.tap_postgres.gitlab_ops_db_labels
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

),
renamed AS (

    SELECT

      id::NUMBER                                AS label_id,
      title                                      AS label_title,
      color,
      source.project_id::NUMBER                 AS project_id,
      group_id::NUMBER                          AS group_id,
      template,
      type                                       AS label_type,
      created_at::TIMESTAMP                      AS created_at,
      updated_at::TIMESTAMP                      AS updated_at

    FROM source

)

SELECT *
FROM renamed