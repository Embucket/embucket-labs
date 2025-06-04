WITH source AS (

    SELECT *
    FROM {{ ref('handbook_values_page_contributors_source') }}

)

SELECT *
FROM source
