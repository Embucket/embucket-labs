{{ simple_cte([
    ('prep_date', 'prep_date'),
    ('prep_charge', 'prep_charge'),
    ('prep_subscription', 'prep_subscription')
])}}

, charges_filtered AS (
/*
Filter to the most recent subscription version for the original subscription/subscription name
*/

  SELECT 
    {{ dbt_utils.star(
           from=ref('prep_charge'), 
           except=['CREATED_BY','UPDATED_BY','MODEL_CREATED_DATE','MODEL_UPDATED_DATE','DBT_UPDATED_AT','DBT_CREATED_AT']
           ) 
      }}
  FROM prep_charge
  WHERE subscription_status NOT IN ('Draft')
      AND charge_type = 'Recurring'
      AND is_included_in_arr_calc = TRUE

), prep_charge_mrr_daily AS (

/*
Expand the most recent subscription version by the effective dates of the charges
This represents the actual effective dates of the products, as updated with each subscription version and carried through as
a history in the charges.

The dim_subscription_id associated with these charges will be the most recent dim_subscription_id in the subscription_name/dim_subscription_id_original lineage.
To find the dim_subscription_id that was active at the time of the charges instead of the most recent version, join this model to the subscription object based on the subscription_name
or dim_subscription_id_original between the subscription created date (adjusted for the first version of a subscription due to backdated effective dates in subscriptions) and the created date of the next subscription version.

Example code:
LEFT JOIN prep_subscription AS subscriptions_charge
    ON prep_charge_mrr_daily.subscription_name = subscriptions_charge.subscription_name
      AND prep_charge_mrr_daily.date_actual BETWEEN subscriptions_charge.subscription_created_datetime_adjusted AND subscriptions_charge.next_subscription_created_datetime
*/

  SELECT
    prep_date.date_actual,
    charges_filtered.*,
    prep_subscription.dim_subscription_id_original
  FROM charges_filtered
  INNER JOIN prep_date
    ON charges_filtered.effective_start_date <= prep_date.date_actual
      AND (charges_filtered.effective_end_date > prep_date.date_actual
        OR charges_filtered.effective_end_date IS NULL)
  LEFT JOIN prep_subscription
    ON charges_filtered.dim_subscription_id = prep_subscription.dim_subscription_id

)

{{ dbt_audit(
    cte_ref="prep_charge_mrr_daily",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2024-09-16",
    updated_date="2024-09-16",
) }}