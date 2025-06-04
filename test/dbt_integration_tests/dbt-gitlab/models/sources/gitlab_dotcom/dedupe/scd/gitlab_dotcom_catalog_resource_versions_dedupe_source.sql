WITH base AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'catalog_resource_versions') }}

)

{{ scd_latest_state() }}
