WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_fill_out_linkedin_lead_gen_form_source') }}

)

SELECT *
FROM source