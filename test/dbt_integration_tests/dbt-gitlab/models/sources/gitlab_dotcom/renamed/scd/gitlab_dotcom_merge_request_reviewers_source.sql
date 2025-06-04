
{{ config({
        "materialized": "table"
        })
}}
    
WITH
{{ distinct_source(source=source('gitlab_dotcom', 'merge_request_reviewers'))}}

, renamed AS (

    SELECT

      id::NUMBER                                        AS merge_request_reviewer_id,
      user_id::NUMBER                                   AS user_id,
      merge_request_id::NUMBER                          AS merge_request_id,
      state::INTEGER                                    AS reviewer_state,
      created_at::TIMESTAMP                             AS created_at,
      valid_from -- Column was added in distinct_source CTE

    FROM distinct_source

)

{{ scd_type_2(
    primary_key_renamed='merge_request_reviewer_id',
    primary_key_raw='id'
) }}
