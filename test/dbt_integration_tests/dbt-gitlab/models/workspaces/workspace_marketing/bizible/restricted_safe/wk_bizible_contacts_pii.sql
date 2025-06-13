WITH source AS (

    SELECT *
    FROM {{ ref('bizible_contacts_source_pii') }}

)

SELECT *
FROM source