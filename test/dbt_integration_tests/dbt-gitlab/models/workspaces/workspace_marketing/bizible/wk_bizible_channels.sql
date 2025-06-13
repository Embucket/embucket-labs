WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_channels_source') }}
    FROM {{ ref('bizible_channels_source') }}

)

SELECT *
FROM source