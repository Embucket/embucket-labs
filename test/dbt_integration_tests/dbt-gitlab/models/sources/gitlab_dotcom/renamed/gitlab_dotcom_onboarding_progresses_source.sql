WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_onboarding_progresses_dedupe_source') }}

), renamed AS (

    SELECT
      id::NUMBER                                    AS onboarding_progress_id,
      namespace_id::NUMBER                          AS namespace_id,
      created_at::TIMESTAMP                         AS created_at,
      updated_at::TIMESTAMP                         AS updated_at,
      merge_request_created_at::TIMESTAMP           AS merge_request_created_at,
      pipeline_created_at::TIMESTAMP                AS pipeline_created_at,
      user_added_at::TIMESTAMP                      AS user_added_at,
      trial_started_at::TIMESTAMP                   AS trial_started_at,
      required_mr_approvals_enabled_at::TIMESTAMP   AS required_mr_approvals_enabled_at,
      code_owners_enabled_at::TIMESTAMP             AS code_owners_enabled_at,
      issue_created_at::TIMESTAMP                   AS issue_created_at,
      secure_dependency_scanning_run_at::TIMESTAMP  AS secure_dependency_scanning_run_at,
      secure_dast_run_at::TIMESTAMP                 AS secure_dast_run_at,
      license_scanning_run_at::TIMESTAMP            AS license_scanning_run_at,
      code_added_at::TIMESTAMP                      AS code_added_at,
      ended_at::TIMESTAMP                           AS ended_at
    FROM source

)

SELECT *
FROM renamed
