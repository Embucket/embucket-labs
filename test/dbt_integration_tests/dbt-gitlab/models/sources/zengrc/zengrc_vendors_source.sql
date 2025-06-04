WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'vendors') }}

),

renamed AS (

    SELECT
      code::VARCHAR                                          AS vendor_code, 
      title::VARCHAR                                         AS vendor_title,
      type::VARCHAR                                          AS vendor_type,   
      description::VARCHAR                                   AS vendor_description,
      id::NUMBER                                             AS vendor_id, 
      notes::VARCHAR                                         AS vendor_notes,
      reference_url::VARCHAR                                 AS vendor_reference_url,
      risk_rating::VARCHAR                                   AS vendor_risk_rating,
      secondary_contact::VARIANT                             AS vendor_secondary_contact,
      status::VARCHAR                                        AS vendor_status,
      tags::VARCHAR                                          AS vendor_tags,
      url::VARCHAR                                           AS vendor_url,
      mapped['data_assets']::VARIANT                         AS mapped_data_assets,
      mapped['facilities']::VARIANT                          AS mapped_facilities,
      mapped['products']::VARIANT                            AS mapped_products,
      mapped['systems']::VARIANT                             AS mapped_systems,
      mapped['tasks']::VARIANT                               AS mapped_tasks,
      PARSE_JSON(custom_attributes)['103']['value']::VARCHAR AS third_party_type,      
      PARSE_JSON(custom_attributes)['210']['value']::VARCHAR AS vendor_security_contact,      
      PARSE_JSON(custom_attributes)['229']['value']::VARCHAR AS critical_system_tier,      
      PARSE_JSON(custom_attributes)['264']['value']::VARCHAR AS data_classification,      
      PARSE_JSON(custom_attributes)['265']['value']::VARCHAR AS processor_controller,      
      PARSE_JSON(custom_attributes)['266']['value']::VARCHAR AS collects_personal_information,      
      PARSE_JSON(custom_attributes)['269']['value']::VARCHAR AS third_party_residual_risk,      
      PARSE_JSON(custom_attributes)['76']['value']::VARCHAR  AS third_party_inherent_risk,  
      start_date::DATE                                       AS vendor_start_date,
      created_at::TIMESTAMP                                  AS vendor_created_at, 
      updated_at::TIMESTAMP                                  AS vendor_updated_at,
      end_date::DATE                                         AS vendor_stop_date,
      _sdc_extracted_at::TIMESTAMP                           AS vendor_loaded_at
    FROM source

)

SELECT *
FROM renamed
