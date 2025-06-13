WITH source AS (

    SELECT *
    FROM {{ ref('bizible_form_submits_source_pii') }}

)

SELECT *
FROM source