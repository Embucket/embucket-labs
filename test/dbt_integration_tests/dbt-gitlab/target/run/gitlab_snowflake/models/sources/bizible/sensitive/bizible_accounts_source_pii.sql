
  
    

create or replace transient table EMBUCKET.sensitive.bizible_accounts_source_pii
    

    
    as (WITH source AS (

    SELECT 

    

    sha2(
        TRIM(
            LOWER(
                account_id || 
                ENCRYPT_RAW(
                  to_binary('SALT', 'utf-8'), 
                  to_binary('4f4a4c7a9c66f0bc70d8acb9c21ff2e9', 'HEX'), 
                  to_binary('416C736F4E637265FFFFFFAB', 'HEX')
                )['ciphertext']::VARCHAR 
            )
        )
    ) AS account_id_hash,



        
    
    
    
    name  
    
    


    FROM EMBUCKET.bizible.bizible_accounts_source

)

SELECT *
FROM source
    )
;


  