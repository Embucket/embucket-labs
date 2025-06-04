{{ config({
        "materialized": "table"
        })
}}

WITH 
{{ distinct_source(source=source('gitlab_dotcom', 'issue_links'))}}

, renamed AS (

    SELECT
      id::NUMBER                      AS issue_link_id,
      source_id::NUMBER               AS source_id,
      target_id::NUMBER               AS target_id,
      created_at::TIMESTAMP            AS created_at,
      updated_at::TIMESTAMP            AS updated_at,
      valid_from -- Column was added in distinct_source CTE

    FROM distinct_source

)

{{ scd_type_2(
    primary_key_renamed='issue_link_id',
    primary_key_raw='id'
) }}
