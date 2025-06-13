WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','user_history') }}
 
), final AS (
 
    SELECT
        TRY_CAST(TO_VARCHAR(updated_at) AS TIMESTAMP)  AS iterable_user_updated_at,    
        TRY_CAST(TO_VARCHAR(updated_at) AS DATE)       AS iterable_user_updated_date,           
        first_name::VARCHAR                            AS iterable_user_first_name,
        last_name::VARCHAR                             AS iterable_user_last_name,
        _fivetran_id::VARCHAR                          AS iterable_user_fivetran_id,
        phone_number::VARCHAR                          AS iterable_user_phone_number,
        user_id::VARCHAR                               AS iterable_user_default_id,           
        TRY_CAST(TO_VARCHAR(signup_date) AS DATE)      AS iterable_user_signup_date,
        TRY_CAST(TO_VARCHAR(signup_date) AS TIMESTAMP) AS iterable_user_signup_at,         
        signup_source::VARCHAR                         AS iterable_user_signup_source,
        email_list_ids::VARIANT                        AS iterable_user_email_list_ids,
        email::VARCHAR                                 AS iterable_user_email,
        phone_number_carrier::VARCHAR                  AS iterable_user_phone_number_carrier,
        phone_number_country_code_iso::VARCHAR         AS iterable_user_phone_number_country_code_iso,
        phone_number_line_type::VARCHAR                AS iterable_user_phone_number_line_type,
        TRY_CAST(TO_VARCHAR(phone_number_updated_at) AS TIMESTAMP) 
                                                       AS iterable_user_phone_number_updated_at, 
        iterable_user_id::VARCHAR                      AS iterable_user_id,
        additional_properties::VARIANT                 AS iterable_user_additional_properties
    FROM source
)

SELECT *
FROM final
