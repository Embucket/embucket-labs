WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','template_history') }}
 
), final AS (
 
    SELECT
        id::VARCHAR                 AS iterable_template_id,
        updated_at::TIMESTAMP       AS iterable_template_updated_at,
        updated_at::DATE            AS iterable_template_updated_date,
        message_type_id::VARCHAR    AS iterable_template_message_type_id,
        client_template_id::VARCHAR AS iterable_template_client_id,
        created_at::TIMESTAMP       AS iterable_template_created_at,
        created_at::DATE            AS iterable_template_created_date,
        creator_user_id::VARCHAR    AS iterable_template_creator_user_id,
        name::VARCHAR               AS iterable_template_name,
        template_type::VARCHAR      AS iterable_template_type
    FROM source

)

SELECT *
FROM final
