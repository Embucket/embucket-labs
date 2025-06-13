WITH source AS (

    SELECT *
    FROM {{ ref('xactly_rule_source') }}

)

SELECT *
FROM source
