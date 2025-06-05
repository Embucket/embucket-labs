
  
    

create or replace transient table EMBUCKET.sensitive.bizible_facts_source_pii
    

    
    as (WITH source AS (

    SELECT 

    

    sha2(
        TRIM(
            LOWER(
                cost_key || 
                ENCRYPT_RAW(
                  to_binary('SALT', 'utf-8'), 
                  to_binary('4f4a4c7a9c66f0bc70d8acb9c21ff2e9', 'HEX'), 
                  to_binary('416C736F4E637265FFFFFFAB', 'HEX')
                )['ciphertext']::VARCHAR 
            )
        )
    ) AS cost_key_hash,



        
    
    
    
    email   , 
    
    
    
    visitor_id  
    
    


    FROM EMBUCKET.bizible.bizible_facts_source

)

SELECT *
FROM source
    )
;


  