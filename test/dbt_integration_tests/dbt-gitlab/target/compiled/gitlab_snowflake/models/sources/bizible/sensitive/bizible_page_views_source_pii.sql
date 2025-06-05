WITH source AS (

    SELECT 

    

    sha2(
        TRIM(
            LOWER(
                page_view_id || 
                ENCRYPT_RAW(
                  to_binary('SALT', 'utf-8'), 
                  to_binary('4f4a4c7a9c66f0bc70d8acb9c21ff2e9', 'HEX'), 
                  to_binary('416C736F4E637265FFFFFFAB', 'HEX')
                )['ciphertext']::VARCHAR 
            )
        )
    ) AS page_view_id_hash,



        
    
    
    
    email   , 
    
    
    
    visitor_id  
    
    


    FROM EMBUCKET.bizible.bizible_page_views_source

)

SELECT *
FROM source