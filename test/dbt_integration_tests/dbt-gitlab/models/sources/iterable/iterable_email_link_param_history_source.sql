WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','email_link_param_history') }}
 
), final AS (
 
    SELECT
        key::VARCHAR            AS iterable_email_link_param_key,
        value::VARCHAR          AS iterable_email_link_param_value,
        template_id::VARCHAR    AS iterable_email_template_id,
        updated_at::TIMESTAMP   AS iterable_email_link_param_updated_at,
        updated_at::DATE        AS iterable_email_link_param_updated_date
    FROM source

)

SELECT *
FROM final
