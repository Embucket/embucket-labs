

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_labels_source') }}

)

SELECT *
FROM source
