WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_pages_domains_source') }}

)

SELECT *
FROM source
