
  
    

create or replace transient table EMBUCKET.sensitive.bizible_customer_ab_tests_source_pii
    

    
    as (WITH source AS (

    SELECT 

    

    sha2(
        TRIM(
            LOWER(
                visitor_id || 
                ENCRYPT_RAW(
                  to_binary('SALT', 'utf-8'), 
                  to_binary('4f4a4c7a9c66f0bc70d8acb9c21ff2e9', 'HEX'), 
                  to_binary('416C736F4E637265FFFFFFAB', 'HEX')
                )['ciphertext']::VARCHAR 
            )
        )
    ) AS visitor_id_hash,



        
    
    
    
    visitor_id  
    
    


    FROM EMBUCKET.bizible.bizible_customer_ab_tests_source

)

SELECT *
FROM source
    )
;


  