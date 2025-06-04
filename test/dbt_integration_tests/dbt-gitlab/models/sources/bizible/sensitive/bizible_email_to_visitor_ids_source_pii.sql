WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_email_to_visitor_ids_source', 'email_to_visitor_id') }}
    FROM {{ ref('bizible_email_to_visitor_ids_source') }}

)

SELECT *
FROM source