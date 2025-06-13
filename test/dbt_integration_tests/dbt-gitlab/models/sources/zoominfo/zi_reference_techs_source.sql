{{ config(
  enabled=false
) }}

WITH source AS (

    SELECT *
    FROM {{ source('zoominfo', 'techs') }}

), renamed AS (

    SELECT 
      zi_c_tech_id::NUMBER                     AS zi_c_tech_id,	           	          
      zi_c_tech_name::VARCHAR  	               AS zi_c_tech_name, 	              
      zi_c_category::VARCHAR  	           	   AS zi_c_category,            
      zi_c_category_parent::VARCHAR  	       AS zi_c_category_parent,       	              
      zi_c_vendor::VARCHAR  	           	   AS zi_c_vendor,            
      zi_c_tech_domain::VARCHAR  	           AS zi_c_tech_domain,   	              
      zi_c_description::VARCHAR                AS zi_c_description
    FROM source

)

SELECT *
FROM renamed