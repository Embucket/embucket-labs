WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_group_audit_events_source') }}

)

SELECT *
FROM source
