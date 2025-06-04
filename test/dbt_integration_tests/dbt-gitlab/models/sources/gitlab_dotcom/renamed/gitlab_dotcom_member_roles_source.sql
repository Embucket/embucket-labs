
WITH final AS (

    SELECT 
      id::INT AS id,
      namespace_id::INT AS namespace_id,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at,
      base_access_level::INT,
      _uploaded_at::FLOAT
    FROM {{ ref('gitlab_dotcom_member_roles_dedupe_source') }} 

)



{{ dbt_audit(
    cte_ref="final",
    created_by="@mpetersen",
    updated_by="@mpetersen",
    created_date="2023-03-20",
    updated_date="2023-03-20"
) }}
