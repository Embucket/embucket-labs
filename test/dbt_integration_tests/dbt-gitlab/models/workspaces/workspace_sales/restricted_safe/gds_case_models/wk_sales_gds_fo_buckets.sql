{{ simple_cte([
    ('dim_crm_user', 'dim_crm_user'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('mart_arr', 'mart_arr'),
    ('dim_date', 'dim_date'),
    ('dim_namespace', 'dim_namespace'),
    ('fct_trial_latest', 'fct_trial_latest'),
    ('bdg_namespace_order_subscription', 'bdg_namespace_order_subscription'),
    ('dim_order', 'dim_order')
    ])

}},

namespaces AS (
  SELECT DISTINCT
    namespace.dim_namespace_id AS namespace_id,
    namespace.creator_id,
    created_at                 AS namespace_created_at,
    CASE is_setup_for_company
      WHEN TRUE THEN 'True'
      WHEN FALSE THEN 'False'
      ELSE 'None'
    END                        AS company_setup_filter,
    namespace.visibility_level AS namespace_visibility_level -- bringing in the visibility level for namespaces
  FROM dim_namespace AS namespace
  WHERE namespace_is_ultimate_parent -- this ensures the subgroup = group namespace 
    AND namespace_is_internal = FALSE -- this blocks internal namespaces
),

namespace_bdg_prep AS (
  SELECT * FROM
    bdg_namespace_order_subscription
  WHERE
    dim_namespace_id = ultimate_parent_namespace_id
    AND product_tier_name_namespace NOT LIKE '%Trial%'
    AND product_tier_name_namespace NOT LIKE '%Free%'
    AND dim_crm_account_id IS NOT NULL
),

paid_namespace_data AS (
  SELECT DISTINCT
    dim_namespace_id                                                                                                      AS paid_namespace_id,
    dim_crm_account_id,
    FIRST_VALUE(dim_subscription_id) OVER (PARTITION BY dim_namespace_id ORDER BY subscription_start_date ASC)            AS first_subscription_id,
    FIRST_VALUE(product_tier_name_subscription) OVER (PARTITION BY dim_namespace_id ORDER BY subscription_start_date ASC) AS first_namespace_product,
    FIRST_VALUE(product_tier_name_order) OVER (PARTITION BY dim_namespace_id ORDER BY subscription_start_date ASC)        AS first_order_product,
    FIRST_VALUE(subscription_start_date) OVER (PARTITION BY dim_namespace_id ORDER BY subscription_start_date ASC)        AS first_subscription_start_date

  FROM namespace_bdg_prep

),

conversions AS (
  SELECT
    paid_namespace_data.* EXCLUDE dim_crm_account_id,
    opps.*,
    CASE
      WHEN opps.product_category LIKE '%Premium%' OR opps.product_details LIKE '%Premium%' THEN 'Premium'
      WHEN opps.product_category LIKE '%Ultimate%' OR opps.product_details LIKE '%Ultimate%' THEN 'Ultimate'
      WHEN opps.product_category LIKE '%Bronze%' OR opps.product_details LIKE '%Bronze%' THEN 'Bronze'
      WHEN opps.product_category LIKE '%Starter%' OR opps.product_details LIKE '%Starter%' THEN 'Starter'
      WHEN opps.product_category LIKE '%Storage%' OR opps.product_details LIKE '%Storage%' THEN 'Storage'
      WHEN opps.product_category LIKE '%Silver%' OR opps.product_details LIKE '%Silver%' THEN 'Silver'
      WHEN opps.product_category LIKE '%Gold%' OR opps.product_details LIKE '%Gold%' THEN 'Gold'
      WHEN opps.product_category LIKE 'CI%' OR opps.product_details LIKE 'CI%' THEN 'CI'
      WHEN opps.product_category LIKE '%omput%' OR opps.product_details LIKE '%omput%' THEN 'CI'
      WHEN opps.product_category LIKE '%Duo%' OR opps.product_details LIKE '%Duo%' THEN 'Duo Pro'
      WHEN opps.product_category LIKE '%uggestion%' OR opps.product_details LIKE '%uggestion%' THEN 'Duo Pro'
      WHEN opps.product_category LIKE '%Agile%' OR opps.product_details LIKE '%Agile%' THEN 'Enterprise Agile Planning'
      ELSE opps.product_category
    END AS product_tier,
    CASE
      WHEN opps.product_category LIKE '%Self%' OR opps.product_details LIKE '%Self%' OR opps.product_category LIKE '%Starter%'
        OR opps.product_details LIKE '%Starter%' THEN 'Self-Managed'
      WHEN opps.product_category LIKE '%SaaS%' OR opps.product_details LIKE '%SaaS%' OR opps.product_category LIKE '%Bronze%'
        OR opps.product_details LIKE '%Bronze%' OR opps.product_category LIKE '%Silver%' OR opps.product_details LIKE '%Silver%'
        OR opps.product_category LIKE '%Gold%' OR opps.product_details LIKE '%Gold%' THEN 'SaaS'
      WHEN opps.product_details NOT LIKE '%SaaS%'
        AND (opps.product_details LIKE '%Premium%' OR opps.product_details LIKE '%Ultimate%') THEN 'Self-Managed'
      WHEN opps.product_category LIKE '%Storage%' OR opps.product_details LIKE '%Storage%' THEN 'Storage'
      ELSE 'Other'
    END AS delivery
  FROM mart_crm_opportunity AS opps
  LEFT JOIN paid_namespace_data
    ON opps.dim_crm_account_id = paid_namespace_data.dim_crm_account_id
  ORDER BY paid_namespace_data.paid_namespace_id ASC
),

--select * from conversions

trials AS (

  SELECT
    dim_namespace_id,
    latest_trial_start_date
  FROM fct_trial_latest

),

combined AS (

  SELECT
    namespaces.*,
    conversions.*,
    trials.latest_trial_start_date,
    COALESCE (trials.dim_namespace_id IS NOT NULL, FALSE)                         AS trial_flag,
    DATEDIFF(DAY, namespaces.namespace_created_at, first_subscription_start_date) AS days_from_creation_to_subscription,
    COALESCE (days_from_creation_to_subscription = 0, FALSE)                      AS paid_from_free_within_day,
    DATEDIFF(DAY, namespaces.namespace_created_at, latest_trial_start_date)       AS days_from_creation_to_trial,
    COALESCE (days_from_creation_to_trial = 0, FALSE)                             AS trial_from_free_within_day,
    DATEDIFF(DAY, latest_trial_start_date, first_subscription_start_date)         AS days_from_trial_to_paid,
    COALESCE (days_from_trial_to_paid = 0, FALSE)                                 AS paid_from_trial_within_day,
    COALESCE (days_from_creation_to_subscription < 0, FALSE)                      AS paid_before_create
  --trial_namespace_id CANNOT TRUST THIS
  FROM conversions
  LEFT JOIN namespaces ON conversions.paid_namespace_id = namespaces.namespace_id
  LEFT JOIN trials ON namespaces.namespace_id = trials.dim_namespace_id
  -- Limit to only conversions after the created date (not created and paid at same time) and trials after creation
  WHERE (trials.dim_namespace_id IS NULL OR trials.latest_trial_start_date >= DATE_TRUNC('day', namespaces.namespace_created_at))
-- and (first_subscription_start_date >= date_trunc('day',namespace_created_at) or first_subscription_start_date is null)

),

full_data AS (
  SELECT
  --date_trunc('week',namespace_created_at) as created_date,
  --date_trunc('week',first_subscription_start_date) as close_date,
    combined.* EXCLUDE (namespace_created_at, latest_trial_start_date, trial_flag),
    CAST(namespace_created_at AS DATE)                           AS namespace_created_at,
    CAST(latest_trial_start_date AS DATE)                        AS latest_trial_start_date,
    COALESCE (latest_trial_start_date IS NOT NULL, FALSE)        AS trial_flag,
    CASE WHEN close_date IS NOT NULL THEN 'Paid' ELSE 'Free' END AS is_paid_flag,
    COALESCE (days_from_creation_to_subscription <= 30, FALSE)   AS convert_within_month_flag
  FROM combined
  WHERE order_type = '1. New - First Order'
    AND is_won
),

--select * from full_data

assign_bucket AS (
  SELECT
    --date_trunc('week',namespace_created_at + 1)-1 as week,
    --  LT_Segment,
    -- fiscal_year,
    -- fiscal_quarter,  
    --  close_month,
    -- is_web_portal_purchase,
    'Buy Now' AS bucket,
    dim_crm_opportunity_id
  FROM full_data
  WHERE
    is_paid_flag = 'Paid'
    AND (close_date >= namespace_created_at AND close_date <= namespace_created_at + 1)
    --and is_web_portal_purchase
    AND delivery LIKE '%SaaS%'
    AND product_tier IS NOT NULL
  UNION ALL
  SELECT
    --date_trunc('week',close_date + 1)-1 as week,
    --    LT_Segment,
    --   fiscal_year,
    --   fiscal_quarter,  
    --  close_month,
    --   is_web_portal_purchase,
    'Trial Convert' AS bucket,
    dim_crm_opportunity_id
  FROM full_data
  WHERE
    is_paid_flag = 'Paid'
    AND close_date > latest_trial_start_date
    AND close_date > namespace_created_at + 1
    --and is_web_portal_purchase
    AND trial_flag
    AND delivery LIKE '%SaaS%'
    AND product_tier IS NOT NULL
    AND DATEDIFF('day', latest_trial_start_date, close_date) <= 40
  UNION ALL
  SELECT
    --date_trunc('week',close_date + 1)-1 as week,
    --    LT_Segment,
    --   fiscal_year,
    --   fiscal_quarter,  
    --  close_month,
    --   is_web_portal_purchase,
    'Free Convert' AS bucket,
    dim_crm_opportunity_id
  FROM full_data
  WHERE
    is_paid_flag = 'Paid'
    AND close_date > namespace_created_at + 1
    --and is_web_portal_purchase
    AND delivery LIKE '%SaaS%'
    AND product_tier IS NOT NULL
    AND (trial_flag = FALSE OR DATEDIFF('day', latest_trial_start_date, close_date) > 40)
  UNION ALL
  SELECT
    --date_trunc('week',close_date + 1)-1 as week,
    --    LT_Segment,
    --   fiscal_year,
    --   fiscal_quarter,  
    --  close_month,
    --   is_web_portal_purchase,
    'Free Convert' AS bucket,
    dim_crm_opportunity_id
  FROM full_data
  WHERE
    --is_paid_flag = 'Paid'
    --and week > date_trunc('week',namespace_created_at + 1)-1
    --is_web_portal_purchase and
    delivery LIKE '%SaaS%'
    AND (namespace_id IS NULL OR paid_before_create = TRUE)
  UNION ALL
  SELECT
    --date_trunc('week',close_date + 1)-1 as week,
    --    LT_Segment,
    --   fiscal_year,
    --   fiscal_quarter,  
    --  close_month,
    --   is_web_portal_purchase,
    'S-M' AS bucket,
    dim_crm_opportunity_id
  FROM conversions
  WHERE --is_web_portal_purchase and 
    delivery LIKE '%Self%'
    AND order_type = '1. New - First Order'
    AND is_won
)

{{ dbt_audit(
    cte_ref="assign_bucket",
    created_by="@mfleisher",
    updated_by="@mfleisher",
    created_date="2024-07-15",
    updated_date="2024-07-15"
) }}
