WITH dim_billing_account AS (

  SELECT *
  FROM {{ ref('dim_billing_account') }}

), dim_crm_account AS (

  SELECT *
  FROM {{ ref('dim_crm_account') }}

), dim_crm_opportunity AS (

  SELECT *
  FROM {{ ref('dim_crm_opportunity') }}

), dim_date AS (

  SELECT *
  FROM {{ ref('dim_date') }}

), dim_product_detail AS (

  SELECT *
  FROM {{ ref('dim_product_detail') }}

), dim_quote AS (

  SELECT *
  FROM {{ ref('dim_quote') }}

), fct_charge AS (

  SELECT *
  FROM {{ ref('wk_finance_fct_recurring_charge_daily_snapshot') }}
  WHERE snapshot_date = '2021-02-04'

), fct_quote_item AS (

  SELECT *
  FROM {{ ref('fct_quote_item') }}

), zuora_subscription AS (

  SELECT *
  FROM {{ ref('zuora_subscription_snapshots_source') }}
  WHERE subscription_status NOT IN ('Draft', 'Expired')
    AND is_deleted = FALSE
    AND exclude_from_analysis IN ('False', '')

), zuora_subscription_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      zuora_subscription.*
    FROM zuora_subscription
    INNER JOIN dim_date snapshot_dates
      ON snapshot_dates.date_actual >= zuora_subscription.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('zuora_subscription.dbt_valid_to') }}
      AND date_actual = '2021-02-04'
    QUALIFY rank() OVER (
         PARTITION BY subscription_name, snapshot_dates.date_actual
         ORDER BY dbt_valid_from DESC) = 1

), opportunity AS (

    SELECT DISTINCT
      dim_crm_opportunity.dim_crm_opportunity_id,
      fct_quote_item.dim_subscription_id
    FROM fct_quote_item
    INNER JOIN dim_crm_opportunity
      ON fct_quote_item.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    INNER JOIN dim_quote
      ON fct_quote_item.dim_quote_id = dim_quote.dim_quote_id
    WHERE stage_name IN ('Closed Won', '8-Closed Lost')
      AND is_primary_quote = TRUE

), renewal_subscriptions AS (

   SELECT DISTINCT
     sub_1.subscription_name,
     sub_1.zuora_renewal_subscription_name,
     DATE_TRUNC('month',sub_2.subscription_end_date::DATE) AS subscription_end_month,
     RANK() OVER (PARTITION BY sub_1.subscription_name ORDER BY sub_1.zuora_renewal_subscription_name, sub_2.subscription_end_date ) AS rank
   FROM zuora_subscription_spined sub_1
   INNER JOIN zuora_subscription_spined sub_2
     ON sub_1.zuora_renewal_subscription_name = sub_2.subscription_name
     AND DATE_TRUNC('month',sub_2.subscription_end_date) >= '2022-02-01'
   WHERE sub_1.zuora_renewal_subscription_name != ''
   QUALIFY rank = 1

), base AS (--get the base data set of recurring charges.

   SELECT
     fct_charge.charge_id,
     fct_charge.dim_crm_account_id,
     fct_charge.dim_billing_account_id,
     opportunity.dim_crm_opportunity_id,
     fct_charge.dim_subscription_id,
     fct_charge.dim_product_detail_id,
     dim_crm_account.parent_crm_account_name,
     dim_crm_account.crm_account_name,
     dim_crm_account.parent_crm_account_sales_segment,
     dim_product_detail.product_tier_name,
     dim_product_detail.product_delivery_type,
     dim_subscription.subscription_name,
     dim_subscription.zuora_renewal_subscription_name,
     dim_subscription.current_term,
     CASE
       WHEN dim_subscription.current_term >= 24 THEN TRUE
       WHEN dim_subscription.subscription_name IN (SELECT DISTINCT subscription_name FROM renewal_subscriptions) THEN TRUE
       ELSE FALSE
     END                                                                                  AS is_myb,
     CASE
       WHEN dim_subscription.subscription_name IN (SELECT DISTINCT subscription_name FROM renewal_subscriptions) THEN TRUE
       ELSE FALSE
     END                                                                                  AS is_myb_with_multi_subs,
     CASE
       WHEN DATE_TRUNC('month',fct_charge.charged_through_date) = fct_charge.effective_end_month THEN TRUE
       ELSE FALSE
     END                                                                                  AS is_paid_in_full,
     CASE
       WHEN charged_through_date IS NULL THEN dim_subscription.current_term
       ELSE DATEDIFF('month',DATE_TRUNC('month',fct_charge.charged_through_date), fct_charge.effective_end_month)
     END AS months_of_future_billings,
     CASE
       WHEN is_paid_in_full = FALSE THEN months_of_future_billings * fct_charge.mrr
       ELSE 0
     END                                                                                  AS estimated_total_future_billings,
     CASE
       WHEN is_paid_in_full = FALSE AND fct_charge.effective_end_month <= '2022-01-01' AND is_myb = TRUE THEN months_of_future_billings * fct_charge.mrr
       ELSE 0
     END                                                                                  AS estimated_fy22_future_billings,
     fct_charge.effective_start_month,
     fct_charge.effective_end_month,
     fct_charge.subscription_start_month,
     fct_charge.subscription_end_month,
     renewal_subscriptions.subscription_end_month                                         AS myb_subscription_end_month,
     DATEDIFF(month,fct_charge.effective_start_month,fct_charge.effective_end_month)      AS charge_term,
     fct_charge.arr
   FROM fct_charge
   LEFT JOIN zuora_subscription_spined dim_subscription
     ON fct_charge.dim_subscription_id = dim_subscription.subscription_id
   LEFT JOIN dim_billing_account
     ON fct_charge.dim_billing_account_id = dim_billing_account.dim_billing_account_id
   LEFT JOIN opportunity
     ON fct_charge.dim_subscription_id = opportunity.dim_subscription_id
   LEFT JOIN dim_crm_account
     ON fct_charge.dim_crm_account_id = dim_crm_account.dim_crm_account_id
   LEFT JOIN renewal_subscriptions
     ON fct_charge.subscription_name = renewal_subscriptions.subscription_name
   LEFT JOIN dim_product_detail
     ON fct_charge.dim_product_detail_id = dim_product_detail.dim_product_detail_id
   WHERE fct_charge.effective_start_month <= '2021-01-01'
     AND fct_charge.effective_end_month > '2021-01-01'

), agg_charge_term_less_than_equal_12 AS (--get the starting and ending month ARR for charges with charge terms <= 12 months. These charges do not need additional logic.

  SELECT
    CASE
      WHEN is_myb = TRUE THEN 'MYB'
      ELSE 'Non-MYB'
    END                             AS renewal_type,
    is_myb,
    is_myb_with_multi_subs,
    current_term,
    charge_term,
    charge_id,
    dim_crm_account_id,
    dim_billing_account_id,
    dim_crm_opportunity_id,
    dim_subscription_id,
    dim_product_detail_id,
    product_tier_name,
    product_delivery_type,
    subscription_name,
    effective_start_month,
    effective_end_month,
    subscription_end_month,
    myb_subscription_end_month,
    SUM(arr)   AS arr
  FROM base
  WHERE charge_term <= 12
  {{ dbt_utils.group_by(n=18) }}

), agg_charge_term_greater_than_12 AS (--get the starting and ending month ARR for charges with charge terms > 12 months. These charges need additional logic.

  SELECT
    CASE
      WHEN is_myb = TRUE THEN 'MYB'
      ELSE 'Non-MYB'
    END                           AS renewal_type,
    is_myb,
    is_myb_with_multi_subs,
    current_term,
    CASE--the below odd term charges do not behave well in the MYB logic and end up with duplicate renewals in the fiscal year. This CASE statement smooths out the charges so they only have one renewal entry in the fiscal year.
      WHEN charge_term = 26 THEN 24
      WHEN charge_term = 28 THEN 24
      WHEN charge_term = 38 THEN 36
      WHEN charge_term = 57 THEN 60
      ELSE charge_term
    END                           AS charge_term,
    charge_id,
    dim_crm_account_id,
    dim_billing_account_id,
    dim_crm_opportunity_id,
    dim_subscription_id,
    dim_product_detail_id,
    product_tier_name,
    product_delivery_type,
    subscription_name,
    effective_start_month,
    effective_end_month,
    subscription_end_month,
    myb_subscription_end_month,
    SUM(arr)   AS arr
  FROM base
  WHERE charge_term > 12
  {{ dbt_utils.group_by(n=18) }}

), twenty_four_mth_term AS (--create records for the intermitent renewals for multi-year charges that are not in the Zuora data. The start and end months are in the agg_myb for MYB.

   SELECT renewal_type, is_myb, is_myb_with_multi_subs, current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id,product_tier_name,product_delivery_type,subscription_name, effective_start_month, DATEADD('month',charge_term/2,effective_start_month) AS effective_end_month,subscription_end_month,myb_subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 13 AND 24 AND effective_end_month > '2022-01-01' {{ dbt_utils.group_by(n=18) }}

), thirty_six_mth_term AS (--create records for the intermitent renewals for MYBs that are not in the Zuora data. The start and end months are in the agg_myb for MYBs.

   SELECT renewal_type, is_myb, is_myb_with_multi_subs, current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id,product_tier_name,product_delivery_type,subscription_name, effective_start_month, DATEADD('month',charge_term/3,effective_start_month) AS effective_end_month,subscription_end_month,myb_subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 25 AND 36 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=18) }}
   UNION ALL
   SELECT renewal_type, is_myb, is_myb_with_multi_subs, current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id,product_tier_name,product_delivery_type,subscription_name, effective_start_month, DATEADD('month',charge_term/3*2,effective_start_month) AS effective_end_month,subscription_end_month,myb_subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 25 AND 36 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=18) }}
   ORDER BY 1

), forty_eight_mth_term AS (--create records for the intermitent renewals for MYBs that are not in the Zuora data. The start and end months are in the agg_myb for MYBs.

   SELECT renewal_type, is_myb, is_myb_with_multi_subs,current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id, product_tier_name,product_delivery_type,subscription_name, effective_start_month, DATEADD('month',charge_term/4,effective_start_month) AS effective_end_month,subscription_end_month,myb_subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 37 AND 48 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=18) }}
   UNION ALL
   SELECT renewal_type, is_myb, is_myb_with_multi_subs,current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id, product_tier_name,product_delivery_type,subscription_name, effective_start_month, DATEADD('month',charge_term/4*2,effective_start_month) AS effective_end_month,subscription_end_month,myb_subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 37 AND 48 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=18) }}
   UNION ALL
   SELECT renewal_type, is_myb, is_myb_with_multi_subs,current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id, product_tier_name,product_delivery_type,subscription_name, effective_start_month, DATEADD('month',charge_term/4*3,effective_start_month) AS effective_end_month,subscription_end_month,myb_subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 37 AND 48 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=18) }}
   ORDER BY 1

), sixty_mth_term AS (--create records for the intermitent renewals for MYBs that are not in the Zuora data. The start and end months are in the agg_myb for MYBs.

   SELECT renewal_type, is_myb, is_myb_with_multi_subs,current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id, product_tier_name,product_delivery_type,subscription_name, effective_start_month, DATEADD('month',charge_term/5,effective_start_month) AS effective_end_month,subscription_end_month,myb_subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 49 AND 60 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=18) }}
   UNION ALL
   SELECT renewal_type, is_myb, is_myb_with_multi_subs,current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id, product_tier_name,product_delivery_type,subscription_name, effective_start_month, DATEADD('month',charge_term/5*2,effective_start_month) AS effective_end_month,subscription_end_month,myb_subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 49 AND 60 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=18) }}
   UNION ALL
   SELECT renewal_type, is_myb, is_myb_with_multi_subs,current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id, product_tier_name,product_delivery_type,subscription_name, effective_start_month, DATEADD('month',charge_term/5*3,effective_start_month) AS effective_end_month,subscription_end_month,myb_subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 49 AND 60 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=18) }}
   UNION ALL
   SELECT renewal_type, is_myb, is_myb_with_multi_subs,current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id, product_tier_name,product_delivery_type,subscription_name, effective_start_month, DATEADD('month',charge_term/5*4,effective_start_month) AS effective_end_month,subscription_end_month,myb_subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 49 AND 60 AND effective_end_month > '2022-01-01' {{ dbt_utils.group_by(n=18) }}
   ORDER BY 1

), combined AS (--union all of the charges

   SELECT * FROM agg_charge_term_less_than_equal_12
   UNION ALL
   SELECT * FROM agg_charge_term_greater_than_12
   UNION ALL
   SELECT * FROM twenty_four_mth_term
   UNION ALL
   SELECT * FROM thirty_six_mth_term
   UNION ALL
   SELECT * FROM forty_eight_mth_term
   UNION ALL
   SELECT * FROM sixty_mth_term

), renewal_report AS (--create the renewal report for the applicable fiscal year.

    SELECT
      dim_date.fiscal_year,
      dim_date.fiscal_quarter_name_fy,
      combined.effective_end_month,
      combined.effective_start_month,
      base.charge_id,
      base.dim_crm_account_id,
      base.dim_billing_account_id,
      base.dim_crm_opportunity_id    AS wip_dim_crm_opportunity_id,
      base.dim_subscription_id,
      base.dim_product_detail_id,
      base.subscription_name,
      base.subscription_start_month,
      base.subscription_end_month,
      base.myb_subscription_end_month,
      base.parent_crm_account_name,
      base.crm_account_name,
      base.parent_crm_account_sales_segment,
      base.product_tier_name,
      base.product_delivery_type,
      combined.renewal_type,
      CASE
        WHEN base.subscription_end_month BETWEEN '2021-02-01' AND '2022-01-01' AND base.is_myb_with_multi_subs = FALSE THEN TRUE
        ELSE FALSE
      END                            AS is_atr,
      base.is_myb,
      base.is_myb_with_multi_subs,
      base.current_term              AS subscription_term,
      base.charge_term,
      base.arr,
      base.estimated_total_future_billings,
      base.estimated_fy22_future_billings
    FROM combined
    LEFT JOIN dim_date
      ON combined.effective_end_month = dim_date.first_day_of_month
    LEFT JOIN base
      ON combined.charge_id = base.charge_id
    WHERE combined.effective_end_month BETWEEN '2021-02-01' AND '2022-01-01'
      AND day_of_month = 1
    ORDER BY fiscal_quarter_name_fy

)

{{ dbt_audit(
    cte_ref="renewal_report",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-03-15",
    updated_date="2021-08-05"
) }}
