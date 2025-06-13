WITH source AS (

    SELECT *
    FROM {{ ref('roles_yaml_source') }}

), filtered as (

    SELECT *
    FROM source
    WHERE rank = 1

)

SELECT *
FROM filtered
