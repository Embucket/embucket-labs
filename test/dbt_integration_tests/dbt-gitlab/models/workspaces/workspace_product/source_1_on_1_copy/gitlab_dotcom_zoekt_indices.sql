WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_zoekt_indices_source') }}

)

SELECT *
FROM source
