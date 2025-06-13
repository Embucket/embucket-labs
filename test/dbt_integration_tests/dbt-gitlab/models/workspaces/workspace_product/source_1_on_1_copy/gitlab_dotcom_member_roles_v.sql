{{ config(
    materialized='view',
    tags=["product"]
) }}

WITH final AS (

    SELECT 
    * EXCLUDE (created_by, updated_by,model_created_date,model_updated_date,dbt_updated_at,dbt_created_at)
    FROM {{ ref('gitlab_dotcom_member_roles_source') }} 

)



{{ dbt_audit(
    cte_ref="final",
    created_by="@mpetersen",
    updated_by="@mpetersen",
    created_date="2023-03-20",
    updated_date="2023-03-20"
) }}
