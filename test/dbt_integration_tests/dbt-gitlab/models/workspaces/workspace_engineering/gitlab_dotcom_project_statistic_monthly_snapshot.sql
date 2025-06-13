WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_project_statistic_historical_monthly') }}
)

SELECT * 
FROM source