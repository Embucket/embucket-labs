

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_plans_source') }}

)

SELECT *
FROM source
