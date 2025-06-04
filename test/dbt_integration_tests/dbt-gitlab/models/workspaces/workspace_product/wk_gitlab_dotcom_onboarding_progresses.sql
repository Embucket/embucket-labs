WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_onboarding_progresses_source') }}

)
SELECT *
FROM source