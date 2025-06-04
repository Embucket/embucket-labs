WITH zuora_accts AS (

    SELECT *
    FROM {{ ref('zuora_account') }}

), zuora_subscriptions_xf AS (

    SELECT *
    FROM {{ ref('zuora_subscription_xf') }}

), zuora_subscription_periods AS (

    SELECT *
    FROM {{ ref('zuora_subscription_periods') }}

), zuora_rp AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan') }}

), zuora_contact AS (
    
    SELECT *
    FROM {{ ref('zuora_contact') }}

), zuora_rpc AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge') }}

), original_rate_plan_id AS (
  
    SELECT DISTINCT
      zuora_rpc.original_id, 
      FIRST_VALUE(subscription_version_term_start_date) OVER (
        PARTITION BY original_id
        ORDER BY periods.version
      ) AS subscription_version_term_start_date,
      FIRST_VALUE(subscription_version_term_end_date) OVER (
        PARTITION BY original_id
        ORDER BY periods.version
      ) AS subscription_version_term_end_date

    FROM zuora_subscription_periods AS periods
      INNER JOIN zuora_rp 
        ON periods.subscription_id  = zuora_rp.subscription_id
      INNER JOIN zuora_rpc 
        ON zuora_rp.rate_plan_id = zuora_rpc.rate_plan_id

), base_mrr AS (

    SELECT
      zuora_rpc.rate_plan_charge_id,
      
      
      -- account info
      zuora_accts.account_name,
      zuora_accts.account_number,
      zuora_contact.country,
      zuora_accts.currency,
      
      -- subscription info
      zuora_subscriptions_xf.subscription_name,
      zuora_subscriptions_xf.subscription_name_slugify,
      zuora_subscriptions_xf.subscription_start_date,
      
      --subscription_lineage info
      zuora_subscriptions_xf.exclude_from_renewal_report,
      zuora_subscriptions_xf.lineage,
      zuora_subscriptions_xf.oldest_subscription_in_cohort,
      zuora_subscriptions_xf.subscription_status,
      
      -- rate_plan info
      zuora_rp.delivery,
      zuora_rp.product_category,
      zuora_rp.rate_plan_name,
      
      --
      zuora_rpc.mrr,
      zuora_rpc.rate_plan_charge_name,
      zuora_rpc.rate_plan_charge_number,
      zuora_rpc.tcv,
      zuora_rpc.unit_of_measure,
      zuora_rpc.quantity,
      
      date_trunc('month', zuora_subscriptions_xf.subscription_start_date :: date) AS sub_start_month,
      date_trunc('month', dateadd('month', -1, zuora_subscriptions_xf.subscription_end_date :: DATE)) AS sub_end_month,
      date_trunc('month', zuora_rpc.effective_start_date :: DATE) AS effective_start_month,
      date_trunc('month', dateadd('month', -1, zuora_rpc.effective_end_date :: DATE)) AS effective_end_month,
      datediff(month, zuora_rpc.effective_start_date :: date,
              zuora_rpc.effective_end_date :: date) AS month_interval,
      zuora_rpc.effective_start_date,
      zuora_rpc.effective_end_date,
      original_rate_plan_id.subscription_version_term_start_date,
      original_rate_plan_id.subscription_version_term_end_date,
      zuora_subscriptions_xf.cohort_month,
      zuora_subscriptions_xf.cohort_quarter
    FROM zuora_accts
    INNER JOIN zuora_subscriptions_xf 
      ON zuora_accts.account_id = zuora_subscriptions_xf.account_id
    INNER JOIN zuora_rp 
      ON zuora_rp.subscription_id = zuora_subscriptions_xf.subscription_id
    INNER JOIN zuora_rpc 
      ON zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id AND zuora_rpc.mrr > 0 AND zuora_rpc.tcv > 0
    LEFT JOIN zuora_contact 
      ON COALESCE(zuora_accts.sold_to_contact_id ,zuora_accts.bill_to_contact_id) = zuora_contact.contact_id
    LEFT JOIN original_rate_plan_id
      ON zuora_rpc.original_id = original_rate_plan_id.original_id

)

SELECT *
FROM base_mrr
WHERE effective_end_month >= effective_start_month
