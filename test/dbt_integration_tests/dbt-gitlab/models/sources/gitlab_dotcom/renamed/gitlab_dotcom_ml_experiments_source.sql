WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ml_experiments_dedupe_source') }}

),

renamed AS (

  SELECT
    id::NUMBER            AS ml_experiment_id,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    iid::NUMBER           AS ml_experiment_iid,
    project_id::NUMBER    AS project_id,
    user_id::NUMBER       AS user_id,
    deleted_on::TIMESTAMP AS deleted_on
  FROM source

)

SELECT *
FROM renamed
