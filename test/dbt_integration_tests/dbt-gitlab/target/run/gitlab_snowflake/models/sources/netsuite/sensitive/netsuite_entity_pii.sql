
  
    

create or replace transient table EMBUCKET.sensitive.netsuite_entity_pii
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.netsuite.netsuite_entity_source

), entity_pii AS (

    SELECT
      entity_id,
      

    

    sha2(
        TRIM(
            LOWER(
                entity_name || 
                ENCRYPT_RAW(
                  to_binary('SALT_NAME', 'utf-8'), 
                  to_binary('4f4a4c7a9c66f0bc70d8acb9c21ff2e9', 'HEX'), 
                  to_binary('416C736F4E637265FFFFFFAB', 'HEX')
                )['ciphertext']::VARCHAR 
            )
        )
    ) AS entity_name_hash,



        
    
    
    
    entity_name   , 
    
    
    
    entity_full_name  
    
    


    FROM source

)

SELECT *
FROM entity_pii
    )
;


  