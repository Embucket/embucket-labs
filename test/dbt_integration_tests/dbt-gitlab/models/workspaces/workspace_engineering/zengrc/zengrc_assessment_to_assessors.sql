WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_assessment_source') }}

)

SELECT
  source.assessment_id,
  assessors.value['id']::NUMBER    AS assessor_id
FROM source
INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(source.assessors)) assessors