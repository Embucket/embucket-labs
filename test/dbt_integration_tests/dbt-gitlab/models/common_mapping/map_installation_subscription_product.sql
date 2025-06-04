{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('prep_charge_mrr_daily_with_installation', 'prep_charge_mrr_daily_with_namespace_and_installation'),
    ('prep_date', 'prep_date')
]) }}

/*

These charges contains a full history of the products associated with a subscription (dim_subscription_id_original/subscription_name) as well as the effective dates of the 
products as they were used by the customer. They are all associated with the most current dim_subscription_id in the subscription_name lineage.

We need to map these charges to the dim_subscription_id at the time the charges were effective otherwise the most recent version will be associated with dates before it was created, based on
how the charges track the history of the subscription. To map between the current dim_subscription_id and the one active at the time of the charges, we join to the subscription object 
between the subscription_created_datetime (adjusted for the first version of a subscription due to backdated effective dates in subscriptions) and the created date of the next subscription version.
*/

SELECT DISTINCT
  prep_charge_mrr_daily_with_installation.date_actual,
  prep_charge_mrr_daily_with_installation.dim_subscription_id,
  prep_charge_mrr_daily_with_installation.dim_subscription_id_original,
  prep_charge_mrr_daily_with_installation.dim_installation_id,
  prep_charge_mrr_daily_with_installation.dim_crm_account_id,
  prep_charge_mrr_daily_with_installation.subscription_version,
  prep_charge_mrr_daily_with_installation.dim_product_detail_id,
  prep_charge_mrr_daily_with_installation.charge_type,
  {{ dbt_utils.generate_surrogate_key([
      'prep_charge_mrr_daily_with_installation.date_actual',
      'prep_charge_mrr_daily_with_installation.dim_installation_id',
      'prep_charge_mrr_daily_with_installation.dim_subscription_id',
      'prep_charge_mrr_daily_with_installation.dim_product_detail_id'
    ])
  }}                                                                     AS primary_key,
  prep_date.is_current_date
FROM prep_charge_mrr_daily_with_installation
LEFT JOIN prep_date
  ON prep_charge_mrr_daily_with_installation.date_actual = prep_date.date_actual
-- Filter out any records where dim_installation_id is missing
WHERE prep_charge_mrr_daily_with_installation.dim_installation_id IS NOT NULL
