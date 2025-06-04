WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_email_to_visitor_ids_source') }}
    FROM {{ ref('bizible_email_to_visitor_ids_source') }}

)

SELECT *
FROM source