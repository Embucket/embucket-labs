WITH source as (

    SELECT *
      FROM {{ source('greenhouse', 'candidate_custom_fields') }}

), renamed as (

    SELECT
            --keys
            candidate_id::NUMBER                AS candidate_id,
            user_id::NUMBER                     AS greenhouse_user_id,

            --info
            custom_field::varchar               AS candidate_custom_field,
            float_value::FLOAT                  AS candidate_custom_field_float_value,
            null           AS candidate_custom_field_date,
            display_value::varchar              AS candidate_custom_field_display_value,
            min_value::NUMBER                   AS candidate_custom_field_min_value,
            max_value::NUMBER                   AS candidate_custom_field_max_value,
            created_at::timestamp               AS candidate_custom_field_created_at,
            updated_at::timestamp               AS candidate_custom_field_updated_at

    FROM source

)

SELECT *
FROM renamed
