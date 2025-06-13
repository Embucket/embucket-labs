WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_merge_request_predictions_dedupe_source') }}

),

renamed AS (

  SELECT

    merge_request_id::NUMBER AS merge_request_id,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    TRY_PARSE_JSON(suggested_reviewers)::VARIANT AS obj_suggested_reviewers,
    obj_suggested_reviewers['top_n']::VARCHAR AS suggested_reviewers_top_n,
    obj_suggested_reviewers['version']::VARCHAR AS suggested_reviewers_version,
    obj_suggested_reviewers['reviewers']::VARIANT AS suggested_reviewers_list

  FROM source

)

SELECT *
FROM renamed
