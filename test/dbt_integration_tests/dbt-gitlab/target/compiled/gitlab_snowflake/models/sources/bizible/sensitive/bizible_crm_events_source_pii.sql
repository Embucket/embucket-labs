WITH source AS (

    SELECT 

    

    sha2(
        TRIM(
            LOWER(
                crm_event_id || 
                ENCRYPT_RAW(
                  to_binary('SALT', 'utf-8'), 
                  to_binary('4f4a4c7a9c66f0bc70d8acb9c21ff2e9', 'HEX'), 
                  to_binary('416C736F4E637265FFFFFFAB', 'HEX')
                )['ciphertext']::VARCHAR 
            )
        )
    ) AS crm_event_id_hash,



        
    
    
    
    lead_email   , 
    
    
    
    contact_email  
    
    


    FROM EMBUCKET.bizible.bizible_crm_events_source

)

SELECT *
FROM source