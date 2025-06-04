{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('mart_arr_all_weekly','mart_arr_weekly_with_zero_dollar_charges'),
    ('mart_ping_instance_metric_weekly', 'mart_ping_instance_metric_weekly'),
    ('dim_subscription', 'dim_subscription'),
    ('gitlab_dotcom_subscription_user_add_on_assignments', 'gitlab_dotcom_subscription_user_add_on_assignments'),
    ('gitlab_dotcom_subscription_add_on_purchases', 'gitlab_dotcom_subscription_add_on_purchases'),
    ('gitlab_dotcom_memberships', 'gitlab_dotcom_memberships'),
    ('dim_product_detail', 'dim_product_detail'),
    ('map_installation_subscription_product', 'map_installation_subscription_product'),
    ('dim_crm_account', 'dim_crm_account'),
    ('wk_rpt_ai_gateway_events_flattened_with_features', 'wk_rpt_ai_gateway_events_flattened_with_features')
	
    ])
}},

all_duo_weekly_seats AS (

  SELECT 
    arr.arr_week
      AS reporting_week,
    arr.subscription_name,
    arr.dim_subscription_id,
    arr.dim_subscription_id_original,
    arr.crm_account_name,
    arr.dim_crm_account_id,
    arr.dim_parent_crm_account_id,
    arr.product_deployment_type
      AS product_deployment,
    SPLIT_PART(arr.product_rate_plan_category, ' - ', 2)
      AS add_on_name,
    SUM(arr.quantity) 
      AS d_seats,
    SUM(arr.arr)
      AS duo_arr,
    IFF(duo_arr > 0, TRUE, FALSE)
      AS is_duo_subscription_paid,
    sub.turn_on_cloud_licensing,
    CASE WHEN sub.turn_on_cloud_licensing = 'Offline' THEN 'Offline Cloud License'
         WHEN sub.turn_on_cloud_licensing = 'No' THEN 'Legacy License File'
         WHEN sub.turn_on_cloud_licensing = 'Yes' THEN 'Standard Cloud License'
         WHEN sub.turn_on_cloud_licensing = '' THEN 'Standard Cloud License'
         ELSE 'error' END
    AS license_type,
    acct.account_owner,
    acct.parent_crm_account_geo,
    acct.parent_crm_account_sales_segment,
    acct.parent_crm_account_industry, 
    acct.technical_account_manager
  FROM mart_arr_all_weekly AS arr
  LEFT JOIN dim_subscription AS sub
    ON arr.dim_subscription_id = sub.dim_subscription_id
  LEFT JOIN dim_crm_account AS acct
    ON acct.dim_crm_account_id = arr.dim_crm_account_id
  WHERE arr_week BETWEEN '2024-02-18' AND CURRENT_DATE -- first duo pro arr
    AND LOWER(product_rate_plan_name) LIKE '%duo%'
  GROUP BY ALL

), 

duo_and_paired_tier AS ( --tier occurring concurrently with paid duo pro subscription

  SELECT 
    duo.*,
    detail.is_oss_or_edu_rate_plan,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT SPLIT_PART(tier.product_tier_name, ' - ', 2)), ', ') -- multiple product tiers can show up within the same ARR reporting week
      AS paired_tier,
    IFF(paired_tier IN ('Premium, Ultimate', 'Ultimate, Premium'), 'Premium & Ultimate', paired_tier)
      AS clean_paired_tier -- not able to sort within group while using SPLIT_PART function - using this method for standard results
  FROM all_duo_weekly_seats AS duo
  LEFT JOIN mart_arr_all_weekly AS tier -- joining to get tier occuring within same week as add on
    ON duo.reporting_week = tier.arr_week
    AND duo.dim_crm_account_id = tier.dim_crm_account_id
    AND duo.dim_subscription_id = tier.dim_subscription_id -- add on will be on the same subscription as the tier
    AND tier.product_category = 'Base Products'
  LEFT JOIN dim_product_detail AS detail
    ON detail.dim_product_detail_id = tier.dim_product_detail_id
  GROUP BY ALL

), 

sm_dedicated_duo_product_info AS ( --CTE purpose is to get product info about sm and dedicated instances even if they don't trigger AI Gateway events

  SELECT DISTINCT
    duo.*,
    s.dim_installation_id
      AS product_entity_id,
    IFF(s.dim_installation_id IS NOT NULL, TRUE, FALSE)
      AS is_product_entity_associated_w_subscription,
    MAX(m.major_minor_version_id)
      AS major_minor_version_id,
    MAX(duo.d_seats)
      AS duo_seats
  FROM duo_and_paired_tier AS duo
  LEFT JOIN map_installation_subscription_product AS s
    ON duo.dim_subscription_id_original = s.dim_subscription_id_original
    AND DATE_TRUNC(week, s.date_actual) = duo.reporting_week
  LEFT JOIN mart_ping_instance_metric_weekly AS m
    ON m.dim_installation_id = s.dim_installation_id
    AND m.ping_created_date_week = DATE_TRUNC(week, s.date_actual)
  WHERE duo.product_deployment IN ('Self-Managed', 'Dedicated')
  GROUP BY ALL

), 

dotcom_duo_product_info AS ( --CTE purpose is to get product info about .com namespaces even if they don't trigger AI Gateway events

  SELECT DISTINCT
    duo.*,
    s.namespace_id
      AS product_entity_id,
    IFF(s.namespace_id IS NOT NULL, TRUE, FALSE)
      AS is_product_entity_associated_w_subscription,
    MAX(m.major_minor_version_id)
      AS major_minor_version_id,
    MAX(duo.d_seats)
      AS duo_seats
  FROM duo_and_paired_tier AS duo
  LEFT JOIN dim_subscription AS s
    ON duo.dim_subscription_id = s.dim_subscription_id
  LEFT JOIN mart_ping_instance_metric_weekly AS m
    ON m.ping_created_date_week = reporting_week
    AND m.dim_installation_id = '8b52effca410f0a380b0fcffaa1260e7' -- installation id for Gitlab.com
  WHERE duo.product_deployment = 'GitLab.com'
  GROUP BY ALL

),

duo_seat_assignments AS ( 
-- Legacy methodology - placeholder code until appropriate to update seat assignment reporting
  SELECT
    m.namespace_id,
    COUNT(DISTINCT a.user_id) 
      AS number_of_seats_assigned,
    p.purchase_xid -- subscription name
  FROM gitlab_dotcom_subscription_user_add_on_assignments AS a
  INNER JOIN gitlab_dotcom_subscription_add_on_purchases AS p
    ON a.add_on_purchase_id = p.id
  INNER JOIN gitlab_dotcom_memberships AS m
    ON a.user_id = m.user_id
     AND p.namespace_id = p.namespace_id
  WHERE (a.pgp_is_deleted = FALSE OR a.pgp_is_deleted IS NULL)
  GROUP BY ALL

),

all_weekly_duo_seats AS (

  SELECT * FROM sm_dedicated_duo_product_info

  UNION ALL

  SELECT * FROM dotcom_duo_product_info

), 

final AS (

  SELECT
    a.reporting_week,
    a.subscription_name,
    a.dim_subscription_id,
    a.crm_account_name,
    a.dim_crm_account_id,
    a.dim_parent_crm_account_id,
    a.product_deployment, 
    a.add_on_name,
    a.clean_paired_tier  
      AS paired_tier,                                                                          
    a.is_product_entity_associated_w_subscription,
    a.is_duo_subscription_paid,
    MAX(a.major_minor_version_id)                                                                  
      AS major_minor_version_id,
    ZEROIFNULL(MAX(a.duo_seats))                                                               
      AS paid_duo_seats,
    MAX(CASE WHEN a.product_deployment = 'GitLab.com' THEN ZEROIFNULL(s.number_of_seats_assigned)
           ELSE null END)            
      AS count_seats_assigned,
    ZEROIFNULL(COUNT(DISTINCT gitlab_global_user_id))
      AS duo_active_users,
    ZEROIFNULL(duo_active_users / paid_duo_seats)
      AS pct_usage_seat_utilization,
    IFF(pct_usage_seat_utilization > 1, 1, pct_usage_seat_utilization)
      AS standard_pct_usage_seat_utilization,
    count_seats_assigned / paid_duo_seats
      AS pct_assignment_seat_utilization,
    IFF(pct_assignment_seat_utilization > 1, 1, pct_assignment_seat_utilization)
      AS standard_pct_assignment_seat_utilization,
    COALESCE(a.is_oss_or_edu_rate_plan, FALSE) 
      AS is_oss_or_edu_rate_plan,
    a.account_owner,
    a.parent_crm_account_geo,
    a.parent_crm_account_sales_segment,
    a.parent_crm_account_industry, 
    a.technical_account_manager,
    a.turn_on_cloud_licensing,
    a.license_type,
    ARRAY_AGG(DISTINCT a.product_entity_id) WITHIN GROUP (ORDER BY a.product_entity_id)
      AS product_entity_array,
    COUNT(DISTINCT a.product_entity_id)
      AS count_product_entities
  FROM all_weekly_duo_seats a
  LEFT JOIN duo_seat_assignments AS s
    ON TO_CHAR(s.namespace_id) = TO_CHAR(a.product_entity_id)
    AND a.product_deployment = 'GitLab.com'
    AND a.subscription_name = s.purchase_xid
  LEFT JOIN wk_rpt_ai_gateway_events_flattened_with_features AS aigw_events
    ON a.product_entity_id = aigw_events.enabled_by_product_entity_id
    AND a.product_deployment = aigw_events.enabled_by_product_deployment_type
    AND DATE_TRUNC(week, aigw_events.behavior_date)::DATE = a.reporting_week
  GROUP BY ALL

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@eneuberger",
    updated_by="@eneuberger",
    created_date="2024-07-22",
    updated_date="2024-02-03"
) }}