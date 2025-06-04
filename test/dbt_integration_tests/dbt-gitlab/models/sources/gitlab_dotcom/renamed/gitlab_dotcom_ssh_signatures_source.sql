WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ssh_signatures_dedupe_source') }}

), parsed_columns AS (

    SELECT
      id::NUMBER                  AS ssh_signatures_id,
      created_at::TIMESTAMP       AS created_at,
      updated_at::TIMESTAMP       AS updated_at,
      project_id::NUMBER          AS project_id,
      verification_status::NUMBER AS verification_status,
      user_id::NUMBER             AS user_id
    FROM source

)

SELECT *
FROM parsed_columns
