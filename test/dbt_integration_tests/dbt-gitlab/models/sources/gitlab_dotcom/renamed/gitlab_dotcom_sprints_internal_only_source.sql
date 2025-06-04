WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_sprints_internal_only_dedupe_source') }}

),

renamed AS (

  SELECT
    id::NUMBER                    AS id,
    created_at::TIMESTAMP         AS created_at,
    updated_at::TIMESTAMP         AS updated_at,
    start_date::DATE              AS start_date,
    due_date::DATE                AS due_date,
    group_id::NUMBER              AS group_id,
    iid::NUMBER                   AS iid,
    title::VARCHAR                AS title,
    description::VARCHAR          AS description,
    iterations_cadence_id::NUMBER AS iterations_cadence_id
  FROM source
)

SELECT *
FROM renamed
