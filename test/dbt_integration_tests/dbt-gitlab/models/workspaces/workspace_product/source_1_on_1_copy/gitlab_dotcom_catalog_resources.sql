WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_catalog_resources_source') }}

)

SELECT *
FROM source
