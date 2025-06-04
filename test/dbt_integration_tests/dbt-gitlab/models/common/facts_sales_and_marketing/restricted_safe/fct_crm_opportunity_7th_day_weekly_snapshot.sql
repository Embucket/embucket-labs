WITH actuals AS (

  SELECT * 
  FROM {{ ref('fct_crm_opportunity_daily_snapshot') }}

),

day_7_list AS (

   -- Filter the data down to only one snapshot every 7 days throughout each quarter.
  {{ date_spine_7th_day() }}

),


final AS (

  SELECT 
    {{ dbt_utils.star(from=ref('fct_crm_opportunity_daily_snapshot'), 
                        except=['CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_UPDATED_AT', 'DBT_CREATED_AT']) }},
  FROM actuals
  INNER JOIN day_7_list
    ON actuals.snapshot_date = day_7_list.day_7

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@lisvinueza",
    updated_by="@lisvinueza",
    created_date="2024-05-09",
    updated_date="2024-06-27"
) }}