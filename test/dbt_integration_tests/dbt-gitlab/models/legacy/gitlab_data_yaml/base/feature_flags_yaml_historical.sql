WITH source AS (

    SELECT *
    FROM {{ ref('feature_flags_source') }}

), filtered AS (

    SELECT *
    FROM source
    WHERE rank = 1

)

SELECT *
FROM filtered
