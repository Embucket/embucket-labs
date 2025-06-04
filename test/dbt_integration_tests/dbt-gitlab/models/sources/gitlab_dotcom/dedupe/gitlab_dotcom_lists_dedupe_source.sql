{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'lists') }}

), partitioned AS (

    SELECT *
    FROM source

    {% if is_incremental() %}

    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id               AS id,
      board_id         AS board_id,
      label_id         AS label_id,
      list_type        AS list_type,
      position         AS position,
      created_at       AS created_at,
      updated_at       AS updated_at,
      user_id::NUMBER  AS user_id,
      milestone_id     AS milestone_id,
      max_issue_count  AS max_issue_count,
      max_issue_weight AS max_issue_weight,
      limit_metric     AS limit_metric,
      _uploaded_at     AS _uploaded_at
    FROM partitioned

)

SELECT *
FROM renamed