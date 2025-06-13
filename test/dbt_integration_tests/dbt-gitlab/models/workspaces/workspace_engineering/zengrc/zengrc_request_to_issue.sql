WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_request_source') }}

)

SELECT
  source.request_id,
  mapped_issues.value['id']::NUMBER     AS issue_id
FROM source
INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(source.mapped_issues)) mapped_issues