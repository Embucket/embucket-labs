WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_namespace_monthly_usages_source') }}

)

SELECT *
FROM source
