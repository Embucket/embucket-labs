{{ simple_cte([
    ('mart_crm_opportunity','mart_crm_opportunity'),
    ('dim_crm_user','dim_crm_user')
    
]) }}

, renewals AS (

    SELECT      
      {{ dbt_utils.star(
           from = ref('mart_crm_opportunity'), 
           except=['CREATED_BY','UPDATED_BY','MODEL_CREATED_DATE','MODEL_UPDATED_DATE','DBT_UPDATED_AT','DBT_CREATED_AT']
           ) 
      }}
    FROM {{ ref('mart_crm_opportunity') }}
    WHERE subscription_type = 'Renewal' 
    AND Stage_name NOT IN 
      (
        'Closed Won',
        '8-Closed Lost',
        '9-Unqualified',
        '10-Duplicate' 
      )
    AND arr_basis_for_clari > 0

), final AS (

    SELECT 
        renewals.*,
        user.user_name as renewal_manager_name 
    FROM renewals
    LEFT JOIN dim_crm_user user
      ON renewals.renewal_manager = user.dim_crm_user_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@jonglee1218",
    created_date="2023-12-01",
    updated_date="2024-02-15",
  ) }}

