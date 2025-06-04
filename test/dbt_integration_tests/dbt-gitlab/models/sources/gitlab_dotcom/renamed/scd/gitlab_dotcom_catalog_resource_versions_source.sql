WITH source AS (

  SELECT *

  FROM {{ ref('gitlab_dotcom_catalog_resource_versions_dedupe_source') }}

),

renamed AS (

  SELECT
    id::INT                    AS id,
    release_id::INT            AS release_id,
    catalog_resource_id::INT   AS catalog_resource_id,
    project_id::INT            AS project_id,
    created_at::TIMESTAMP      AS created_at,
    released_at::TIMESTAMP     AS released_at,
    semver_major::INT          AS semver_major,
    semver_minor::INT          AS semver_minor,
    semver_patch::INT          AS semver_patch,
    semver_prerelease::VARCHAR AS semver_prerelease
  FROM source

)


SELECT *
FROM renamed
