WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_issue_source') }}

)

SELECT
  source.issue_id,
  mapped_audits.value['id']::NUMBER     AS audit_id
FROM source
INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(source.mapped_audits)) mapped_audits