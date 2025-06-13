WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_issuable_severities_dedupe_source') }}

), renamed AS (

    SELECT
      id::NUMBER            AS issue_severity_id,
      issue_id::NUMBER      AS issue_id,
      severity::NUMBER      AS severity

    FROM source

)


SELECT *
FROM renamed