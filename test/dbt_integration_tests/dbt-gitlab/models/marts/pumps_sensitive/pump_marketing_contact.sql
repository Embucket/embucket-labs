{{ config(
    tags=["product", "mnpi_exception"]
) }}

SELECT

  -- MAPPED COLUMNS: ANY ADDITIONAL COLUMNS SHOULD BE ADDED TO THE END OF THIS LIST
  company_name,
  country,
  customer_db_confirmed_date,
  customer_db_created_date,
  customer_db_customer_id,
  days_since_saas_signup_bucket,
  days_since_saas_trial_ended_bucket,
  days_since_self_managed_owner_signup_bucket,
  dim_marketing_contact_id,
  email_address,
  first_name,
  gitlab_dotcom_active_state,
  gitlab_dotcom_confirmed_date,
  gitlab_dotcom_created_date,
  IFNULL(gitlab_dotcom_email_opted_in, FALSE) AS gitlab_dotcom_email_opted_in,
  gitlab_dotcom_last_login_date,
  gitlab_dotcom_user_id,
  gitlab_user_name,
  group_member_of_saas_bronze_tier,
  group_member_of_saas_free_tier,
  group_member_of_saas_premium_tier,
  group_member_of_saas_trial,
  group_member_of_saas_ultimate_tier,
  group_owner_of_saas_bronze_tier,
  group_owner_of_saas_free_tier,
  group_owner_of_saas_premium_tier,
  group_owner_of_saas_trial,
  group_owner_of_saas_ultimate_tier,
  individual_namespace_is_saas_bronze_tier,
  individual_namespace_is_saas_free_tier,
  individual_namespace_is_saas_premium_tier,
  individual_namespace_is_saas_trial,
  individual_namespace_is_saas_ultimate_tier,
  is_customer_db_owner,
  is_customer_db_user,
  is_gitlab_dotcom_user,
  is_group_namespace_member,
  is_group_namespace_owner,
  is_individual_namespace_owner,
  is_paid_tier,
  is_self_managed_premium_tier,
  is_self_managed_starter_tier,
  is_self_managed_ultimate_tier,
  is_zuora_billing_contact,
  last_name,
  responsible_for_free_tier_only,
  responsible_for_group_saas_bronze_tier,
  responsible_for_group_saas_free_tier,
  responsible_for_group_saas_premium_tier,
  responsible_for_group_saas_trial,
  responsible_for_group_saas_ultimate_tier,
  zuora_active_state,
  zuora_contact_id,
  zuora_created_date,
  pql_list_stages,
  pql_nbr_stages,
  pql_nbr_namespace_users,
  has_namespace_setup_for_company_use,
  pql_namespace_id,
  pql_namespace_name,
  pql_nbr_integrations_installed,
  pql_integrations_installed,
  pql_namespace_creator_job_description,
  is_pql,

  is_member_of_public_ultimate_parent_namespace,
  is_member_of_private_ultimate_parent_namespace,

  --Ptpt fields
  is_ptpt_contact,
  ptpt_namespace_id,
  ptpt_score_group,
  ptpt_insights,
  ptpt_score_date,

  -- Namespace notification dates
  user_limit_notification_at,
  user_limit_enforcement_at,
  is_impacted_by_user_limit,
  user_limit_namespace_id,

  marketo_lead_id,

    --Ptp fields
  is_ptp_contact,
  ptp_namespace_id,
  ptp_score_group,
  ptp_insights,
  ptp_past_insights,
  ptp_score_date,
  ptp_past_score_group,
  ptp_days_since_trial_start,
  ptp_source,
  is_group_maintainer_of_saas_paid_tier,

  -- METADATA COLUMNS FOR USE IN PUMP (NOT INTEGRATION)
  last_changed

FROM {{ ref('mart_marketing_contact' )}}
WHERE rlike(email_address, '^[A-Z0-9.+_%-]+@[A-Z0-9.-]+\\.[A-Z]+$','i')
  AND ( is_pql_change = TRUE
    OR ( is_paid_tier_change = TRUE
      AND sfdc_record_id IS NOT NULL
    )
    OR is_ptpt_contact_change = TRUE
    OR is_impacted_by_user_limit_change = TRUE
    OR is_ptp_contact_change = TRUE
  )
