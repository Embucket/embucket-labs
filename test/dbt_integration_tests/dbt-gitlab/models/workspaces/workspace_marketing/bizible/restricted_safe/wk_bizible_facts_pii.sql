WITH source AS (

    SELECT *
    FROM {{ ref('bizible_facts_source_pii') }}

)

SELECT *
FROM source