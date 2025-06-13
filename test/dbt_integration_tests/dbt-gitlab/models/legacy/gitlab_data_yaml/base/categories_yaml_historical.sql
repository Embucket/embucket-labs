WITH source AS (

    SELECT *
    FROM {{ ref('categories_yaml_source') }}

), filtered as (

    SELECT *
    FROM source
    WHERE rank = 1

)

SELECT *
FROM filtered
