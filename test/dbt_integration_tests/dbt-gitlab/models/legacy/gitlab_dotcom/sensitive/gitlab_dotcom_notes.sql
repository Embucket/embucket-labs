

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_notes_source') }}

)

SELECT *
FROM source
