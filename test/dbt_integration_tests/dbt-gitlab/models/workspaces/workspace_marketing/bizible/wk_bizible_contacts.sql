WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_contacts_source') }}
    FROM {{ ref('bizible_contacts_source') }}

)

SELECT *
FROM source