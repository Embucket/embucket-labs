WITH source AS (

    SELECT {{ hash_sensitive_columns('marketo_activity_fill_out_form_source') }}
    FROM {{ ref('marketo_activity_fill_out_form_source') }}

)

SELECT *
FROM source