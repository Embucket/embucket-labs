WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_ops_labels_source') }}

)

SELECT *
FROM source
