WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_catalog_resource_versions_source') }}

)

SELECT *
FROM source
