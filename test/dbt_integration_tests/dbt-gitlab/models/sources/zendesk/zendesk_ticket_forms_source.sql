WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'ticket_forms') }}
    
), flattened AS (

    SELECT

      id::NUMBER                                AS ticket_form_id,
      active::BOOLEAN                           AS is_form_active,
      "DEFAULT"::BOOLEAN                        AS is_default,
      display_name::VARCHAR                     AS display_name,
      end_user_visible::BOOLEAN                 AS is_end_user_visible,
      in_all_brands::BOOLEAN                    AS is_in_all_brands,
      name::VARCHAR                             AS name,
      position::NUMBER                          AS position,
      raw_display_name::VARCHAR                 AS raw_display_name,
      raw_name::VARCHAR                         AS raw_name,
      restricted_brand_ids::VARCHAR             AS restricted_brand_ids,
      ticket_field_ids::VARCHAR                 AS ticket_field_ids,
      updated_at::TIMESTAMP                     AS updated_at,
      created_at::TIMESTAMP                     AS created_at

    FROM source

)

SELECT *
FROM flattened


