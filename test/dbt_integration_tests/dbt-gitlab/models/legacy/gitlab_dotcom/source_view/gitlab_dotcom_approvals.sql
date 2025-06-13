

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_approvals_source') }}

)

SELECT *
FROM source
