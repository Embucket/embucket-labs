WITH source AS (

    SELECT *
    FROM {{ ref('stages_yaml_source') }}

), max_date AS (

    SELECT *
    FROM source
    WHERE snapshot_date = (SELECT max(snapshot_date) FROM source)

)

SELECT *
FROM max_date