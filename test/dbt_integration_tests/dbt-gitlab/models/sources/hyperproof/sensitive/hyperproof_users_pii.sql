WITH source AS (

    SELECT {{ nohash_sensitive_columns('hyperproof_users_source', 'email') }}
    FROM {{ ref('hyperproof_users_source') }}

)

SELECT *
FROM source