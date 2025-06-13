{{ simple_cte([
    ('prep_date', 'prep_date'),
    ('prep_charge', 'prep_charge')
]) }}

, mrr AS (

    SELECT
      prep_date.date_id,
      {{ dbt_utils.star(
           from=ref('prep_charge'), 
           except=['CREATED_BY','UPDATED_BY','MODEL_CREATED_DATE','MODEL_UPDATED_DATE','DBT_UPDATED_AT','DBT_CREATED_AT']
           ) 
      }}
    FROM prep_charge
    INNER JOIN prep_date
      ON prep_charge.effective_start_month = prep_date.date_actual
    WHERE prep_charge.subscription_status NOT IN ('Draft')
      AND prep_charge.charge_type = 'Recurring'
      /* This excludes Education customers (charge name EDU or OSS) with free subscriptions.
         Pull in seats from Paid EDU Plans with no ARR */
      AND (prep_charge.mrr != 0 OR LOWER(prep_charge.rate_plan_charge_name) = 'max enrollment')
      AND prep_charge.is_included_in_arr_calc = TRUE

)

{{ dbt_audit(
    cte_ref="mrr",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2023-08-14",
    updated_date="2023-08-14",
) }}
