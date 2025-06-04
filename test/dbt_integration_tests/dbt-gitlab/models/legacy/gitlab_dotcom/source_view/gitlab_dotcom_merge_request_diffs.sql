

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_merge_request_diffs_source') }}

)

SELECT *
FROM source
