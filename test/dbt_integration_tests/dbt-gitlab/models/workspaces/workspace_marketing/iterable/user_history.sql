WITH source AS (

    SELECT *
    FROM {{ ref('iterable_user_history_source') }}

)

SELECT *
FROM source