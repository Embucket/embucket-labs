WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ml_candidates_dedupe_source') }}

),

renamed AS (

  SELECT
    id::NUMBER                                         AS ml_candidate_id,
    created_at::TIMESTAMP                              AS created_at,
    updated_at::TIMESTAMP                              AS updated_at,
    experiment_id::NUMBER                              AS experiment_id,
    user_id::NUMBER                                    AS user_id,
    DATEADD('ms', start_time, '1970-01-01')::TIMESTAMP AS start_at,
    DATEADD('ms', end_time, '1970-01-01')::TIMESTAMP   AS end_at,
    status::NUMBER                                     AS status,
    package_id::NUMBER                                 AS package_id,
    eid::TEXT                                          AS ml_candidate_eid,
    project_id::NUMBER                                 AS project_id,
    internal_id::NUMBER                                AS ml_candidate_internal_id,
    ci_build_id::NUMBER                                AS ci_build_id
  FROM source

)

SELECT *
FROM renamed
