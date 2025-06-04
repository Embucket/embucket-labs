WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_assessment_source') }}

)

SELECT
  source.assessment_id,
  audits.value['id']::NUMBER     AS audit_id
FROM source
INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(source.mapped_audits)) audits