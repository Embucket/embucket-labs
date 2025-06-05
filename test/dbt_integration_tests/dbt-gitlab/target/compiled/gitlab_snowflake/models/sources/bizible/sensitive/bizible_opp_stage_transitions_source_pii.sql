WITH source AS (

    SELECT 

    

    sha2(
        TRIM(
            LOWER(
                opp_stage_transition_id || 
                ENCRYPT_RAW(
                  to_binary('SALT', 'utf-8'), 
                  to_binary('4f4a4c7a9c66f0bc70d8acb9c21ff2e9', 'HEX'), 
                  to_binary('416C736F4E637265FFFFFFAB', 'HEX')
                )['ciphertext']::VARCHAR 
            )
        )
    ) AS opp_stage_transition_id_hash,



        
    
    
    
    email  
    
    


    FROM EMBUCKET.bizible.bizible_opp_stage_transitions_source

)

SELECT *
FROM source