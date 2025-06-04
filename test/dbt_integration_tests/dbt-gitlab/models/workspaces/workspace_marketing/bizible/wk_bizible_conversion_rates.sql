WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_conversion_rates_source') }}
    FROM {{ ref('bizible_conversion_rates_source') }}

)

SELECT *
FROM source