{{ simple_cte([
    ('mart_crm_opportunity','mart_crm_opportunity'),
    ('dim_crm_user','dim_crm_user')
    
]) }}

, open_opportunities AS (

    SELECT      
      {{ dbt_utils.star(
           from = ref('mart_crm_opportunity'), 
           except=['CREATED_BY','UPDATED_BY','MODEL_CREATED_DATE','MODEL_UPDATED_DATE','DBT_UPDATED_AT','DBT_CREATED_AT']
           ) 
      }}
    FROM {{ ref('mart_crm_opportunity') }}
    WHERE Stage_name NOT IN 
      (
        'Closed Won',
        '8-Closed Lost',
        '9-Unqualified',
        '10-Duplicate' 
      )
    AND arr_basis_for_clari > 0

), final AS (

    SELECT 
        open_opportunities.*,
        user.user_name as renewal_manager_name 
    FROM open_opportunities
    LEFT JOIN dim_crm_user user
      ON open_opportunities.renewal_manager = user.dim_crm_user_id

)

SELECT * 
FROM final