WITH source AS (

    SELECT *
    FROM EMBUCKET.netsuite.netsuite_subsidiaries_source

), entity_pii AS (

    SELECT
      subsidiary_id,
      

    

    sha2(
        TRIM(
            LOWER(
                subsidiary_name || 
                ENCRYPT_RAW(
                  to_binary('SALT_NAME', 'utf-8'), 
                  to_binary('4f4a4c7a9c66f0bc70d8acb9c21ff2e9', 'HEX'), 
                  to_binary('416C736F4E637265FFFFFFAB', 'HEX')
                )['ciphertext']::VARCHAR 
            )
        )
    ) AS subsidiary_name_hash,



        
    
    
    
    subsidiary_full_name   , 
    
    
    
    subsidiary_name  
    
    


    FROM source

)

SELECT *
FROM entity_pii