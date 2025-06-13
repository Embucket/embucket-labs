WITH source AS (

    SELECT *
    FROM {{ ref('roles_yaml_historical') }}

), max_date AS (

    SELECT *
    FROM source
    WHERE snapshot_date = (SELECT max(snapshot_date) FROM source)

)

SELECT *
FROM max_date