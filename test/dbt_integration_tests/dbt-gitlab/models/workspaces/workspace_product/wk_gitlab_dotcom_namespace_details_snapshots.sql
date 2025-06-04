
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_namespace_details_snapshots_source') }}

)

SELECT *
FROM source
