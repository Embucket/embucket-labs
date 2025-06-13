WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_fill_out_form_source_pii') }}

)

SELECT *
FROM source