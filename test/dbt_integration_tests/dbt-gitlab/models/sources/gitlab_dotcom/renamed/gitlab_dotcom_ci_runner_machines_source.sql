WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_runner_machines_dedupe_source') }}

),

renamed AS (

  SELECT
    id::NUMBER            AS id,
    runner_id::NUMBER     AS runner_id,
    executor_type::NUMBER AS executor_type,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    version::VARCHAR      AS version,
    runner_type::NUMBER   AS runner_type

  FROM source

)


SELECT *
FROM renamed
