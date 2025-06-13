WITH source AS (

    SELECT *
    FROM {{ ref('bizible_email_to_visitor_ids_source_pii') }}

)

SELECT *
FROM source