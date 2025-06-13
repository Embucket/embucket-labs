WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'rejection_question_custom_fields') }}

), renamed as (

	SELECT

            --key
            application_id::NUMBER			AS application_id,
            user_id::NUMBER             AS user_id,
            
            --info
            custom_field::varchar               AS rejection_question_custom_field,
            float_value::float                  AS rejection_question_custom_field_float_value,
            TRY_TO_DATE(date_value::VARCHAR)    AS rejection_question_custom_field_date_value,
            display_value::varchar              AS rejection_question_custom_field_display_value,
            unit::varchar                       AS rejection_question_custom_field_unit,
            min_value::NUMBER                  AS rejection_question_custom_field_min_value,
            max_value::NUMBER                  AS rejection_question_custom_field_max_value,
            created_at::timestamp               AS rejection_question_custom_field_created_at,
            updated_at::timestamp               AS rejection_question_custom_field_updated_at

	FROM source

)

SELECT *
FROM renamed
