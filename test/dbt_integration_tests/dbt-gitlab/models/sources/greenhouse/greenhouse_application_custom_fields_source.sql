WITH source as (

	SELECT *
  	FROM {{ source('greenhouse', 'application_custom_fields') }}

), renamed as (

    SELECT

            --keys
            application_id::NUMBER                  AS application_id,
            user_id::NUMBER                         AS user_id,

            --info
            custom_field::varchar                   AS  application_custom_field,
            float_value::float                      AS  application_custom_field_float_value,
            display_value::varchar                  AS  application_custom_field_display_value,
            unit::varchar                           AS  application_custom_field_unit,
            min_value::NUMBER                       AS  application_custom_field_min_value,
            max_value::NUMBER                       AS  application_custom_field_max_value,
            TRY_TO_DATE(date_value::VARCHAR)        AS  application_custom_field_date,
            created_at::timestamp                   AS  application_custom_field_created_at,
            updated_at::timestamp                   AS  application_custom_field_updated_at

   	FROM source

)

SELECT *
FROM renamed
