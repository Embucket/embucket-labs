
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_member_roles_source
    

    
    as (WITH final AS (

    SELECT 
      id::INT AS id,
      namespace_id::INT AS namespace_id,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at,
      base_access_level::INT,
      _uploaded_at::FLOAT
    FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_member_roles_dedupe_source 

)



SELECT
      *,
      '@mpetersen'::VARCHAR       AS created_by,
      '@mpetersen'::VARCHAR       AS updated_by,
      '2023-03-20'::DATE        AS model_created_date,
      '2023-03-20'::DATE        AS model_updated_date,
      CURRENT_TIMESTAMP()               AS dbt_updated_at,

    

        

            CURRENT_TIMESTAMP()               AS dbt_created_at

        
    

    FROM final
    )
;


  