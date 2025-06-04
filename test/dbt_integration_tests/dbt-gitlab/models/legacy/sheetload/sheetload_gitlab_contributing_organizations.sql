WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_gitlab_contributing_organizations_source') }}

)

SELECT *
FROM source
