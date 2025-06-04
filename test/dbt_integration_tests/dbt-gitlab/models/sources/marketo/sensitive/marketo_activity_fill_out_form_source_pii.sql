WITH source AS (

    SELECT {{ nohash_sensitive_columns('marketo_activity_fill_out_form_source', 'marketo_activity_fill_out_form_id') }}
    FROM {{ ref('marketo_activity_fill_out_form_source') }}

)

SELECT *
FROM source