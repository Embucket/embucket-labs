

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_group_group_links_source') }}

)

SELECT *
FROM source
