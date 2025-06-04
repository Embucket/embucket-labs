WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'ticket_fields') }}
    
), flattened AS (

    SELECT

      id::NUMBER                                    AS ticket_field_id,
      active::BOOLEAN                               AS is_field_active,
      description::VARCHAR                          AS description,
      agent_description::VARCHAR                    AS agent_description,
      position::NUMBER                              AS field_position,
      raw_description::VARCHAR                      AS raw_description,
      raw_title::VARCHAR                            AS raw_title,
      raw_title_in_portal::VARCHAR                  AS raw_title_in_portal,
      regexp_for_validation::VARCHAR                AS regexp_for_validation,
      sub_type_id::NUMBER                           AS sub_type_id,
      system_field_options::VARCHAR                 AS system_field_options,
      tag::VARCHAR                                  AS tag,
      title::VARCHAR                                AS ticket_form_title,
      title_in_portal::VARCHAR                      AS title_in_portal,
      type::VARCHAR                                 AS type,
      url::VARCHAR                                  AS url,
      custom_field_options::VARIANT                 AS custom_field_options,
      removable::BOOLEAN                            AS is_removable,
      required::BOOLEAN                             AS is_required,
      required_in_portal::BOOLEAN                   AS is_required_in_portal,
      editable_in_portal::BOOLEAN                   AS is_editable_in_portal,
      visible_in_portal::BOOLEAN                    AS is_visible_in_portal,
      collapsed_for_agents::BOOLEAN                 AS is_collapsed_for_agents,
      updated_at::TIMESTAMP                         AS updated_at,
      created_at::TIMESTAMP                         AS created_at

    FROM source

)

SELECT *
FROM flattened
