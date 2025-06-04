WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_form_submits_source') }}
    FROM {{ ref('bizible_form_submits_source') }}

)

SELECT *
FROM source