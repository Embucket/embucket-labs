WITH source AS (

    SELECT 

    

    sha2(
        TRIM(
            LOWER(
                account_to_email_id || 
                ENCRYPT_RAW(
                  to_binary('SALT_EMAIL', 'utf-8'), 
                  to_binary('4f4a4c7a9c66f0bc70d8acb9c21ff2e9', 'HEX'), 
                  to_binary('416C736F4E637265FFFFFFAB', 'HEX')
                )['ciphertext']::VARCHAR 
            )
        )
    ) AS account_to_email_id_hash,



        
    
    
    
    account_to_email_id   , 
    
    
    
    email  
    
    


    FROM EMBUCKET.bizible.bizible_account_to_emails_source

)

SELECT *
FROM source