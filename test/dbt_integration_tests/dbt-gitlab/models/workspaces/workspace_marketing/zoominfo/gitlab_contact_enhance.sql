WITH source AS (

    SELECT {{ hash_sensitive_columns('gitlab_contact_enhance_source') }}
    FROM {{ ref('gitlab_contact_enhance_source') }}

)

SELECT *
FROM source

