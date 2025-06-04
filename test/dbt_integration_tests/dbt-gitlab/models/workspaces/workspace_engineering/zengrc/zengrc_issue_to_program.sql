WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_issue_source') }}

)

SELECT
  source.issue_id,
  mapped_programs.value['id']::NUMBER     AS program_id
FROM source
INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(source.mapped_programs)) mapped_programs