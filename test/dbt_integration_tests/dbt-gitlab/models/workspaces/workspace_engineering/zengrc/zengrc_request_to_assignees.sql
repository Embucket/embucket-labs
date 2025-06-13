WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_request_source') }}

)

SELECT
  source.request_id,
  assignees.value['id']::NUMBER     AS assignee_id
FROM source
INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(source.assignees)) assignees