WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_gpg_signatures_dedupe_source') }}

), parsed_columns AS (

    SELECT
      id::NUMBER                  AS gpg_signatures_id,
      created_at::TIMESTAMP       AS created_at,
      updated_at::TIMESTAMP       AS updated_at,
      project_id::NUMBER          AS project_id,
      gpg_key_id::NUMBER          AS gpg_key_id,
      verification_status::NUMBER AS verification_status
    FROM source

)

SELECT *
FROM parsed_columns
