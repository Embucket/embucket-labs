WITH source AS (

    SELECT *
    FROM {{ ref('zi_comp_with_linkages_global_source') }}

)

SELECT *
FROM source