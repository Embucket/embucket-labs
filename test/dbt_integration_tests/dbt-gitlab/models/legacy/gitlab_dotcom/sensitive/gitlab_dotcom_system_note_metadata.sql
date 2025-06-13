

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_system_note_metadata_source') }}

)

SELECT *
FROM source
