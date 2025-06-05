

WITH source AS
(

    SELECT *
    FROM EMBUCKET.commonroom.organizations
    QUALIFY ROW_NUMBER() OVER (PARTITION BY domain, organization_name ORDER BY _uploaded_at DESC, _file_name DESC) = 1

), source_pii AS (

    SELECT 

    

    sha2(
        TRIM(
            LOWER(
                organization_name || 
                ENCRYPT_RAW(
                  to_binary('SALT_NAME', 'utf-8'), 
                  to_binary('4f4a4c7a9c66f0bc70d8acb9c21ff2e9', 'HEX'), 
                  to_binary('416C736F4E637265FFFFFFAB', 'HEX')
                )['ciphertext']::VARCHAR 
            )
        )
    ) AS organization_name_hash,



        
    
    
    
    location   , 
    
    
    
    organization_name  
    
    


    FROM source
)

SELECT DISTINCT *
  FROM source_pii