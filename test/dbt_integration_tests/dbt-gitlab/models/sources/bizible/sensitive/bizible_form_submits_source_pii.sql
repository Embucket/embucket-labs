WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_form_submits_source', 'form_submit_id') }}
    FROM {{ ref('bizible_form_submits_source') }}

)

SELECT *
FROM source