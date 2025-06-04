{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('fct_available_to_renew', 'fct_available_to_renew'),
    ('prep_ramp_subscription', 'prep_ramp_subscription'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_billing_account', 'dim_billing_account'),

]) }},

sub_version AS

( 

--marking the last version with row number

SELECT 
dim_subscription.dim_subscription_id,
dim_subscription.dim_crm_opportunity_id,
dim_subscription.subscription_name,
dim_subscription.subscription_start_date,
dim_subscription.subscription_end_date,
dim_subscription.dim_crm_account_id,
dim_subscription.subscription_version,
dim_subscription.zuora_renewal_subscription_name,
row_number () OVER (PARTITION BY subscription_name ORDER BY subscription_version DESC) AS row_num,
dim_billing_account.crm_entity,
dim_billing_account.dim_billing_account_id
FROM dim_subscription
LEFT JOIN dim_billing_account ON dim_billing_account.dim_billing_account_id = dim_subscription.dim_billing_account_id

), 

last_sub_version AS 

( 

--picking the last version

SELECT *
FROM sub_version
WHERE row_num = 1

),

cancelled_sub AS

( 

--in last versions determine subscriptions cancelled per start date

SELECT *
FROM last_sub_version
WHERE last_sub_version.subscription_start_date = last_sub_version.subscription_end_date

),

first_version_non_cancelled AS

( 

--joining to remove subscriptions cancelled per start date and filtering version 1 only for renewal subscriptions

SELECT
sub_version.dim_subscription_id,
sub_version.dim_crm_opportunity_id,
sub_version.subscription_name,
sub_version.subscription_start_date,
sub_version.subscription_end_date,
sub_version.dim_crm_account_id,
sub_version.subscription_version,
sub_version.row_num,
sub_version.crm_entity,
sub_version.dim_billing_account_id
FROM sub_version
LEFT JOIN cancelled_sub ON cancelled_sub.subscription_name = sub_version.subscription_name
WHERE cancelled_sub.subscription_name IS NULL
AND sub_version.subscription_version = 1

),

 
-- Available to Renew (ATR) Calculation for all Quarters along with Renewal Linkage Subscriptions

renewal_linkage AS ( 

    SELECT DISTINCT
      fiscal_year,
      fiscal_quarter_name_fy, 
      dim_crm_account_id, 
      dim_crm_opportunity_id,
      dim_subscription_id, 
      subscription_name,
      LEAD(dim_subscription_id) OVER (PARTITION BY subscription_name ORDER BY atr_term_end_date) AS renewal_subscription_id,
      renewal_subscription_name,
      renewal_month,
      dim_billing_account_id,
      dim_parent_crm_account_id,
      parent_crm_account_name,
      atr_term_start_date,
      atr_term_end_date,
      dim_crm_user_id,
      user_name,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      arr, 
      quantity
    FROM fct_available_to_renew 
    
), final AS (

SELECT
      renewal_linkage.fiscal_year,
      renewal_linkage.fiscal_quarter_name_fy, 
      renewal_linkage.dim_crm_account_id, 
      renewal_linkage.dim_crm_opportunity_id,
      renewal_linkage.dim_subscription_id, 
      renewal_linkage.subscription_name,
      COALESCE(renewal_linkage.renewal_subscription_id, first_version_non_cancelled.dim_subscription_id) AS renewal_subscription_id,
      renewal_linkage.renewal_subscription_name,
      renewal_linkage.renewal_month,
      prep_ramp_subscription.ramp_type,
      prep_ramp_subscription.is_last_in_lineage,
      renewal_linkage.dim_billing_account_id,
      renewal_linkage.dim_parent_crm_account_id,
      renewal_linkage.parent_crm_account_name,
      renewal_linkage.atr_term_start_date,
      renewal_linkage.atr_term_end_date,
      renewal_linkage.dim_crm_user_id,
      renewal_linkage.user_name,
      renewal_linkage.crm_user_sales_segment,
      renewal_linkage.crm_user_geo,
      renewal_linkage.crm_user_region,
      renewal_linkage.crm_user_area,
      SUM(renewal_linkage.arr) AS arr, 
      SUM(renewal_linkage.quantity) AS quantity
FROM renewal_linkage
LEFT JOIN first_version_non_cancelled 
  ON first_version_non_cancelled.subscription_name = renewal_linkage.renewal_subscription_name
LEFT JOIN prep_ramp_subscription
  ON prep_ramp_subscription.dim_subscription_id = renewal_linkage.dim_subscription_id
{{ dbt_utils.group_by(22) }}
ORDER BY 3,1 ASC )

{{ dbt_audit(
cte_ref="final",
created_by="@snalamaru",
updated_by="@apiaseczna",
created_date="2024-04-01",
updated_date="2025-01-30"
) }}


