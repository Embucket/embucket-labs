WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_reviews_dedupe_source') }}

), parsed_columns AS (

    SELECT
      id::NUMBER                  AS reviews_id,
      created_at::TIMESTAMP       AS created_at,
      author_id::NUMBER           AS author_id,
      merge_request_id::NUMBER    AS merge_request_id,
      project_id::NUMBER          AS project_id
    FROM source

)

SELECT *
FROM parsed_columns
