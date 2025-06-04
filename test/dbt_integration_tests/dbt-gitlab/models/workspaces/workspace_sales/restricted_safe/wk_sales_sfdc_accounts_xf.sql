{{ config(alias='sfdc_accounts_xf') }}

-- use warehouse reporting;

WITH raw_account AS (

  SELECT *
  FROM {{ source('salesforce', 'account') }}
  --FROM raw.salesforce_v2_stitch.account

), mart_crm_account AS (

    SELECT mart_account.*
    --FROM prod.restricted_safe_common_mart_sales.mart_crm_account mart_account
    FROM {{ref('mart_crm_account')}} mart_account

), account_owner AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_users_xf')}}
    --FROM prod.workspace_sales.sfdc_users_xf

), sfdc_record_type AS (
    -- using source in prep temporarily
    SELECT *
    FROM {{ref('sfdc_record_type_source')}}
    --FROM PREP.sfdc.sfdc_record_type_source
),
 sfdc_users_xf AS (

    SELECT *
    --FROM prod.workspace_sales.sfdc_users_xf
    FROM {{ref('wk_sales_sfdc_users_xf')}}
 )

SELECT
    mart.dim_crm_account_id                                  AS account_id,
    mart.crm_account_name                                    AS account_name,
    mart.master_record_id,
    mart.dim_crm_user_id                                     AS owner_id,



    parent_account_owner.business_unit                       AS parent_account_owner_user_business_unit,
    parent_account_owner.sub_business_unit                   AS parent_account_owner_user_sub_business_unit,
    parent_account_owner.division                            AS parent_account_owner_user_division,
    parent_account_owner.asm                                 AS parent_account_owner_user_asm,

    parent_account_owner.role_type                           AS parent_account_owner_user_role_type,

    ------------------------
    mart.dim_crm_person_primary_contact_id                   AS primary_contact_id,
    mart.record_type_id,
    mart.partner_vat_tax_id,
    mart.gitlab_com_user,
    mart.account_manager,
    mart.account_owner,
    mart.crm_business_dev_rep_id,
    mart.dedicated_service_engineer,
    mart.crm_sales_dev_rep_id,
    mart.technical_account_manager_id,
    mart.dim_parent_crm_account_id                           AS ultimate_parent_account_id,
    mart.crm_account_type                                    AS account_type,
    mart.crm_account_industry                                AS industry,
    mart.crm_account_sub_industry                            AS sub_industry,
    mart.parent_crm_account_industry                         AS parent_account_industry,
    mart.account_tier,
    mart.customer_since_date,
    mart.carr_this_account,
    mart.carr_account_family,
    mart.next_renewal_date,
    mart.license_utilization,
    mart.support_level,
    mart.named_account,
    mart.crm_account_billing_country                         AS billing_country,
    mart.crm_account_billing_country_code                    AS billing_country_code,
    mart.billing_postal_code,
    mart.is_sdr_target_account,
    mart.parent_crm_account_lam                              AS lam,
    mart.parent_crm_account_lam_dev_count                    AS lam_dev_count,
    mart.is_jihu_account,
    mart.partners_signed_contract_date,
    mart.partner_account_iban_number,
    mart.partner_type,
    mart.partner_status,
    mart.is_first_order_available,
    mart.crm_account_zi_technologies                         AS zi_technologies,
    mart.technical_account_manager_date,
    mart.gitlab_customer_success_project,
    mart.forbes_2000_rank,
    mart.potential_users,
    mart.number_of_licenses_this_account,
    mart.decision_maker_count_linkedin,
    mart.number_of_employees,
    mart.account_phone,
    mart.zoominfo_account_phone,
    mart.admin_manual_source_number_of_employees,
    mart.admin_manual_source_account_address,
    mart.parent_crm_account_sales_segment,
    mart.parent_crm_account_geo,
    mart.parent_crm_account_region,
    mart.parent_crm_account_area,
    mart.parent_crm_account_territory,
    mart.crm_account_employee_count,
    mart.parent_crm_account_max_family_employee,
    mart.parent_crm_account_upa_country,
    mart.parent_crm_account_upa_state,
    mart.parent_crm_account_upa_city,
    mart.parent_crm_account_upa_street,
    mart.parent_crm_account_upa_postal_code,
    mart.parent_crm_account_business_unit,
    mart.health_number,
    mart.health_score_color,
    mart.count_active_subscription_charges,
    mart.count_active_subscriptions,
    mart.count_billing_accounts,
    mart.count_licensed_users,
    mart.count_of_new_business_won_opportunities,
    mart.count_open_renewal_opportunities,
    mart.count_opportunities,
    mart.count_products_purchased,
    mart.count_won_opportunities,
    mart.count_concurrent_ee_subscriptions,
    mart.count_ce_instances,
    mart.count_active_ce_users,
    mart.count_open_opportunities,
    mart.count_using_ce,
    mart.abm_tier,
    mart.crm_account_gtm_strategy                             AS gtm_strategy,
    mart.gtm_acceleration_date,
    mart.gtm_account_based_date,
    mart.gtm_account_centric_date,
    mart.abm_tier_1_date,
    mart.abm_tier_2_date,
    mart.abm_tier_3_date,
    mart.demandbase_account_list,
    mart.demandbase_intent,
    mart.demandbase_page_views,
    mart.demandbase_score,
    mart.demandbase_sessions,
    mart.demandbase_trending_offsite_intent,
    mart.demandbase_trending_onsite_engagement,
    mart.is_locally_managed_account,
    mart.is_strategic_account,
    mart.partner_track,
    mart.partners_partner_type,
    mart.gitlab_partner_program,
    mart.zoom_info_company_name,
    mart.zoom_info_company_revenue,
    mart.zoom_info_company_employee_count,
    mart.zoom_info_company_industry,
    mart.zoom_info_company_city,
    mart.zoom_info_company_state_province,
    mart.zoom_info_company_country,
    mart.is_excluded_from_zoom_info_enrich,
    mart.crm_account_zoom_info_website                              AS zoom_info_website,
    mart.crm_account_zoom_info_company_other_domains                AS zoom_info_company_other_domains,
    mart.crm_account_zoom_info_dozisf_zi_id                         AS zoom_info_dozisf_zi_id,
    mart.crm_account_zoom_info_parent_company_zi_id                 AS zoom_info_parent_company_zi_id,
    mart.crm_account_zoom_info_parent_company_name                  AS zoom_info_parent_company_name,
    mart.crm_account_zoom_info_ultimate_parent_company_zi_id        AS zoom_info_ultimate_parent_company_zi_id,
    mart.crm_account_zoom_info_ultimate_parent_company_name         AS zoom_info_ultimate_parent_company_name,
    mart.crm_account_zoom_info_number_of_developers                 AS zoom_info_number_of_developers,
    mart.crm_account_zoom_info_total_funding                        AS zoom_info_total_funding,
    mart.is_key_account,
    mart.created_by_id,
    mart.crm_account_created_date                                   AS created_date,
    mart.is_deleted,
    mart.last_modified_by_id,
    mart.last_modified_date,
    mart.last_activity_date,
    mart.dbt_updated_at                                             AS _last_dbt_run,
    mart.technical_account_manager,
    mart.parent_crm_account_name                                    AS ultimate_parent_account_name,

    sfdc_record_type.record_type_name,
    sfdc_record_type.business_process_id,
    sfdc_record_type.record_type_label,
    sfdc_record_type.record_type_description,
    sfdc_record_type.record_type_modifying_object_type,

    mart.is_zi_jenkins_present                                      AS zi_jenkins_presence_flag,
    mart.is_zi_svn_present                                          AS zi_svn_presence_flag,
    mart.is_zi_tortoise_svn_present                                 AS zi_tortoise_svn_presence_flag,
    mart.is_zi_gcp_present                                          AS zi_gcp_presence_flag,
    mart.is_zi_atlassian_present                                    AS zi_atlassian_presence_flag,
    mart.is_zi_github_present                                       AS zi_github_presence_flag,
    mart.is_zi_github_enterprise_present                            AS zi_github_enterprise_presence_flag,
    mart.is_zi_aws_present                                          AS zi_aws_presence_flag,
    mart.is_zi_kubernetes_present                                   AS zi_kubernetes_presence_flag,
    mart.is_zi_apache_subversion_present                            AS zi_apache_subversion_presence_flag,
    mart.is_zi_apache_subversion_svn_present                        AS zi_apache_subversion_svn_presence_flag,
    mart.is_zi_hashicorp_present                                    AS zi_hashicorp_presence_flag,
    mart.is_zi_aws_cloud_trail_present                              AS zi_aws_cloud_trail_presence_flag,
    mart.is_zi_circle_ci_present                                    AS zi_circle_ci_presence_flag,
    mart.is_zi_bit_bucket_present                                   AS zi_bit_bucket_presence_flag,

    -- fields from RAW table
    '' AS parent_crm_account_upa_country_name,

    CASE
        WHEN mart.parent_crm_account_lam_dev_count BETWEEN 0 AND 25
            THEN '[0-25]'
        WHEN mart.parent_crm_account_lam_dev_count BETWEEN 26 AND 100
            THEN '(25-100]'
        WHEN mart.parent_crm_account_lam_dev_count BETWEEN 101 AND 250
            THEN '(100-250]'
        WHEN mart.parent_crm_account_lam_dev_count BETWEEN 251 AND 1000
            THEN '(250-1000]'
        WHEN mart.parent_crm_account_lam_dev_count BETWEEN 1001 AND 2500
            THEN '(1000-2500]'
        WHEN mart.parent_crm_account_lam_dev_count > 2500
            THEN '(2500+]'
    END                     AS lam_dev_count_bin,

    --------------------
    --------------------
    -- RAW ACCOUNT FIELDS
    raw_acc.parent_lam_industry_acct_heirarchy__c   AS hierarcy_industry,
    --raw_acc.has_tam__c                              AS has_tam_flag,
    raw_acc.public_sector_account__c                AS public_sector_account_flag,
    raw_acc.pubsec_type__c                          AS pubsec_type,
    raw_acc.lam_tier__c                             AS account_lam_arr,
    raw_acc.lam_dev_count__c                        AS account_lam_dev_count,
    raw_acc.parent_lam_industry_acct_heirarchy__c   AS account_industry,

    -- For segment calculation we leverage the upa logic
    raw_upa.pubsec_type__c                          AS upa_pubsec_type,
    raw_upa.lam_tier__c                             AS upa_lam_arr,
    raw_upa.lam_dev_count__c                        AS upa_lam_dev_count,


    raw_upa.parent_lam_industry_acct_heirarchy__c   AS upa_industry,

    -- account owner fields
    acc_owner.business_unit                              AS account_owner_user_business_unit,
    acc_owner.sub_business_unit                          AS account_owner_user_sub_business_unit,
    acc_owner.division                                   AS account_owner_user_division,
    acc_owner.asm                                        AS account_owner_user_asm,
    acc_owner.adjusted_user_segment                      AS account_owner_user_segment,

    UPPER(acc_owner.user_geo)                           AS account_owner_user_geo,
    acc_owner.user_region                               AS account_owner_user_region,

    -- NF: Add the logic for hybrid users
    -- If hybrid user we leverage the account demographics data
    CASE
        WHEN acc_owner.is_hybrid_flag = 1
            THEN mart.parent_crm_account_area
        ELSE acc_owner.user_area
    END                                                     AS account_owner_user_area,

    -- NF: Add the logic for hybrid users
    -- If hybrid user we leverage the account demographics data
    CASE
        WHEN acc_owner.is_hybrid_flag = 1
        THEN mart.parent_crm_account_sales_segment
        ELSE acc_owner.user_segment
    END                                                 AS account_owner_raw_user_segment,

    acc_owner.role_type                                 AS account_owner_user_role_type,


    -----------------------------------
    -- upa owner fields

    upa_owner.user_id                             AS upa_owner_id,
    upa_owner.name                                AS upa_owner_name,
    raw_upa.name                                  AS upa_name,

    upa_owner.business_unit                       AS upa_owner_user_business_unit,
    upa_owner.sub_business_unit                   AS upa_owner_user_sub_business_unit,
    upa_owner.division                            AS upa_owner_user_division,
    upa_owner.asm                                 AS upa_owner_user_asm,
    upa_owner.adjusted_user_segment               AS upa_owner_user_segment,

    UPPER(upa_owner.user_geo)                     AS upa_owner_user_geo,
    upa_owner.user_region                         AS upa_owner_user_region,

    -- NF: Add the logic for hybrid users
    -- If hybrid user we leverage the account demographics data
    CASE
        WHEN upa_owner.is_hybrid_flag = 1
            THEN mart.parent_crm_account_area
        ELSE upa_owner.user_area
    END                                            AS upa_owner_user_area,

    -- NF: Add the logic for hybrid users
    -- If hybrid user we leverage the account demographics data
    CASE
        WHEN upa_owner.is_hybrid_flag = 1
        THEN mart.parent_crm_account_sales_segment
        ELSE upa_owner.user_segment
    END                                             AS upa_owner_raw_user_segment,

    upa_owner.role_type                             AS upa_owner_user_role_type,

   -- Raw fields

    raw_acc.billingstate                            AS account_billing_state_name,
    raw_acc.billingstatecode                        AS account_billing_state_code,
    raw_acc.billingcountry                          AS account_billing_country_name,
    raw_acc.billingcountrycode                      AS account_billing_country_code,
    raw_acc.billingcity                             AS account_billing_city,
    raw_acc.billingpostalcode                       AS account_billing_postal_code,


    raw_acc.parentid                                AS parent_id,

    -- this fields might not be upa level
    raw_acc.account_demographics_business_unit__c        AS account_demographics_business_unit,
    UPPER(raw_acc.account_demographics_geo__c)           AS account_demographics_geo,
    raw_acc.account_demographics_region__c               AS account_demographics_region,
    raw_acc.account_demographics_area__c                 AS account_demographics_area,
    UPPER(raw_acc.account_demographics_sales_segment__c) AS account_demographics_sales_segment,
    raw_acc.account_demographics_territory__c            AS account_demographics_territory,

    raw_upa.account_demographics_business_unit__c        AS account_demographics_upa_business_unit,
    UPPER(raw_upa.account_demographics_geo__c)           AS account_demographics_upa_geo,
    raw_upa.account_demographics_region__c               AS account_demographics_upa_region,
    raw_upa.account_demographics_area__c                 AS account_demographics_upa_area,
    UPPER(raw_upa.account_demographics_sales_segment__c) AS account_demographics_upa_sales_segment,
    raw_upa.account_demographics_territory__c            AS account_demographics_upa_territory,

    raw_upa.account_demographics_upa_state__c            AS account_demographics_upa_state_code,
    raw_upa.account_demographics_upa_state_name__c       AS account_demographics_upa_state_name,
    raw_upa.account_demographics_upa_country_name__c     AS account_demographics_upa_country_name,
    raw_upa.account_demographics_upa_country__c          AS account_demographics_upa_country_code,
    raw_upa.account_demographics_upa_city__c             AS account_demographics_upa_city,
    raw_upa.account_demographics_upa_postal_code__c      AS account_demographics_upa_postal_code,
    raw_upa.account_demographic_max_family_employees__c  AS account_demographics_upa_max_family_employees,


    -- fields from mart account
    --mart_crm_account.public_sector_account_flag,
    raw_acc.customer_score__c                            AS customer_score,


    COALESCE(raw_acc.decision_maker_count_linkedin__c,0)        AS linkedin_developer,
    COALESCE(raw_acc.zi_revenue__c,0)                           AS zi_revenue,
    COALESCE(raw_acc.account_demographics_employee_count__c,0)  AS account_demographics_employees,
    COALESCE(raw_acc.carr_acct_family__c,0)                     AS account_family_arr,
    LEAST(50000,GREATEST(COALESCE(raw_acc.number_of_licenses_this_account__c,0),COALESCE(raw_acc.potential_users__c, raw_acc.decision_maker_count_linkedin__c , raw_acc.zi_number_of_developers__c, 0)))           AS calculated_developer_count,


    
    -- fy25 keys

     LOWER(account_owner_user_geo)                                                                                                     AS key_geo,
     LOWER(account_owner_user_geo || '_' || account_owner_user_business_unit)                                                          AS key_geo_bu,
     LOWER(account_owner_user_geo || '_' || account_owner_user_business_unit || '_' || account_owner_user_region)                      AS key_geo_bu_region,
     LOWER(account_owner_user_geo || '_' || account_owner_user_business_unit || '_' || account_owner_user_region|| '_' || account_owner_user_area)   AS key_geo_bu_region_area

FROM mart_crm_account AS mart
LEFT JOIN sfdc_record_type
    ON mart.record_type_id = sfdc_record_type.record_type_id
LEFT JOIN account_owner parent_account_owner
    ON parent_account_owner.user_id = mart.dim_crm_user_id
INNER JOIN raw_account AS raw_acc
    ON raw_acc.id = mart.dim_crm_account_id
-- upa account demographics fields
LEFT JOIN raw_account AS raw_upa
    ON raw_upa.id = mart.dim_parent_crm_account_id
LEFT JOIN sfdc_users_xf AS acc_owner
    ON raw_acc.ownerid = acc_owner.user_id
-- upa owner id doesn't seem to be on mart crm
LEFT JOIN sfdc_users_xf AS upa_owner
    ON raw_upa.ownerid = upa_owner.user_id
WHERE mart.is_deleted = FALSE

--------------------
--------------------
--------------------