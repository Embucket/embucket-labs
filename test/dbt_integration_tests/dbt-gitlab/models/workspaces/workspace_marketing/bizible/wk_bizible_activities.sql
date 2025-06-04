WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_activities_source') }}
    FROM {{ ref('bizible_activities_source') }}

)

SELECT *
FROM source