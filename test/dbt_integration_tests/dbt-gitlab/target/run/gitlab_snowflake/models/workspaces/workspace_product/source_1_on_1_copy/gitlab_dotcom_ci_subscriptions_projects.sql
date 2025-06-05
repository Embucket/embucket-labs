
  
    

create or replace transient table EMBUCKET.workspace_product.gitlab_dotcom_ci_subscriptions_projects
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_ci_subscriptions_projects_source

)

SELECT *
FROM source
    )
;


  