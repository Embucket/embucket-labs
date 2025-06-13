WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_runners_dedupe_source') }}
  WHERE created_at IS NOT NULL

), renamed AS (

  SELECT
    id::NUMBER                                     AS runner_id,
    created_at::TIMESTAMP                          AS created_at,
    updated_at::TIMESTAMP                          AS updated_at,
    description::VARCHAR                           AS description,
    contacted_at::TIMESTAMP                        AS contacted_at,
    active::BOOLEAN                                AS is_active,
    version::VARCHAR                               AS version,
    revision::VARCHAR                              AS revision,
    platform::VARCHAR                              AS platform,
    run_untagged::BOOLEAN                          AS is_untagged,
    locked::BOOLEAN                                AS is_locked,
    access_level::NUMBER                           AS access_level,
    maximum_timeout::NUMBER                        AS maximum_timeout,
    runner_type::NUMBER                            AS runner_type,
    public_projects_minutes_cost_factor::FLOAT     AS public_projects_minutes_cost_factor,
    private_projects_minutes_cost_factor::FLOAT    AS private_projects_minutes_cost_factor
  FROM source

)

SELECT *
FROM renamed
