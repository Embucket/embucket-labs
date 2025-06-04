{%- macro sfdc_sandbox_account_fields(model_type) %}

WITH map_merged_crm_account AS (

    SELECT *
    FROM {{ ref('map_merged_crm_account') }}

), prep_crm_person AS (

    SELECT *
    FROM {{ ref('prep_crm_person') }}

), sfdc_user_roles_source AS (

    SELECT *
    FROM {{ ref('sfdc_user_roles_source') }}

), dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), crm_user AS (

    SELECT * 
    FROM
    {%- if model_type == 'live' %}
        {{ ref('wk_prep_crm_user') }}
    {%- elif model_type == 'snapshot' %}
        {{ ref('wk_prep_crm_user_daily_snapshot') }}
    {% endif %}

{%- if model_type == 'live' %}

{%- elif model_type == 'snapshot' %}
), snapshot_dates AS (

    SELECT *
    FROM dim_date
    WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE
    {% if is_incremental() %}

   -- this filter will only be applied on an incremental run
   AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

{% endif %}

), lam_corrections AS (

    SELECT
      snapshot_dates.date_id                  AS snapshot_id,
      dim_parent_crm_account_id               AS dim_parent_crm_account_id,
      dev_count                               AS dev_count,
      estimated_capped_lam                    AS estimated_capped_lam,
      dim_parent_crm_account_sales_segment    AS parent_crm_account_sales_segment
    FROM {{ ref('driveload_lam_corrections_source') }}
    INNER JOIN snapshot_dates
        ON snapshot_dates.date_actual >= valid_from
          AND snapshot_dates.date_actual < COALESCE(valid_to, '9999-12-31'::TIMESTAMP)

{%- endif %}

), sfdc_account AS (

    SELECT
    {%- if model_type == 'live' %}
        *
    {%- elif model_type == 'snapshot' %}
        {{ dbt_utils.generate_surrogate_key(['sfdc_account_snapshots_source.account_id','snapshot_dates.date_id'])}}   AS crm_account_snapshot_id,
        snapshot_dates.date_id                                                                                AS snapshot_id,
        snapshot_dates.date_actual                                                                            AS snapshot_date,
        snapshot_dates.fiscal_year                                                                            AS snapshot_fiscal_year,
        sfdc_account_snapshots_source.*
     {%- endif %}
    FROM
    {%- if model_type == 'live' %}
        {{ ref('sfdc_account_source') }}
    {%- elif model_type == 'snapshot' %}
        {{ ref('sfdc_account_snapshots_source') }}
         INNER JOIN snapshot_dates
           ON snapshot_dates.date_actual >= sfdc_account_snapshots_source.dbt_valid_from
           AND snapshot_dates.date_actual < COALESCE(sfdc_account_snapshots_source.dbt_valid_to, '9999-12-31'::TIMESTAMP)
    {%- endif %}
    WHERE account_id IS NOT NULL
    {%- if model_type == 'snapshot' %}
      {% if is_incremental() %}

      AND snapshot_date > (SELECT MAX(snapshot_date) FROM {{this}})

      {% endif %}
      
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY 
          snapshot_id, 
          account_id 
        ORDER BY dbt_valid_from DESC
        ) = 1

    {% endif %}

), sfdc_users AS (

    SELECT
      {%- if model_type == 'live' %}
        *
      {%- elif model_type == 'snapshot' %}
      {{ dbt_utils.generate_surrogate_key(['sfdc_user_snapshots_source.user_id','snapshot_dates.date_id'])}}    AS crm_user_snapshot_id,
      snapshot_dates.date_id                                                                           AS snapshot_id,
      sfdc_user_snapshots_source.*
      {%- endif %}
    FROM
      {%- if model_type == 'live' %}
      {{ ref('sfdc_users_source') }}
      {%- elif model_type == 'snapshot' %}
      {{ ref('sfdc_user_snapshots_source') }}
       INNER JOIN snapshot_dates
         ON snapshot_dates.date_actual >= sfdc_user_snapshots_source.dbt_valid_from
         AND snapshot_dates.date_actual < COALESCE(sfdc_user_snapshots_source.dbt_valid_to, '9999-12-31'::TIMESTAMP)
    {%- endif %}

), sfdc_record_type AS (

    SELECT *
    FROM {{ ref('sfdc_record_type') }}

), pte_scores AS (

    SELECT 
      crm_account_id                                                                                           AS account_id,
      score                                                                                                    AS score,
      decile                                                                                                   AS decile,
      score_group                                                                                              AS score_group,
      MIN(score_date)                                                                                          AS valid_from,
      COALESCE(LEAD(valid_from) OVER (PARTITION BY crm_account_id ORDER BY valid_from), {{ var('tomorrow') }}) AS valid_to,
      CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY crm_account_id ORDER BY valid_from DESC) = 1 
          THEN TRUE
        ELSE FALSE
      END                                                                                                      AS is_current
    FROM {{ ref('pte_scores_source') }}
    {{ dbt_utils.group_by(n=4)}}
    ORDER BY valid_from, valid_to


), ptc_scores AS (

    SELECT 
      crm_account_id                                                                                           AS account_id,
      score                                                                                                    AS score,
      decile                                                                                                   AS decile,
      score_group                                                                                              AS score_group,
      MIN(score_date)                                                                                          AS valid_from,
      COALESCE(LEAD(valid_from) OVER (PARTITION BY crm_account_id ORDER BY valid_from), {{ var('tomorrow') }}) AS valid_to,
      CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY crm_account_id ORDER BY valid_from DESC) = 1 
          THEN TRUE
        ELSE FALSE
      END                                                                                                      AS is_current
    FROM {{ ref('ptc_scores_source') }}
    {{ dbt_utils.group_by(n=4)}}
    ORDER BY valid_from, valid_to

), current_fiscal_year AS (

    SELECT fiscal_year
    FROM dim_date
    WHERE date_actual = CURRENT_DATE()

), final AS (

    SELECT
      --crm account information
      {%- if model_type == 'live' %}

      {%- elif model_type == 'snapshot' %}
      sfdc_account.crm_account_snapshot_id,
      sfdc_account.snapshot_id,
      sfdc_account.snapshot_date,
      {%- endif %}
      --primary key
      sfdc_account.account_id                                             AS dim_crm_account_id,

      --surrogate keys
      sfdc_account.ultimate_parent_account_id                             AS dim_parent_crm_account_id,
      sfdc_account.owner_id                                               AS dim_crm_user_id,
      map_merged_crm_account.dim_crm_account_id                           AS merged_to_account_id,
      sfdc_account.record_type_id                                         AS record_type_id,
      account_owner.user_id                                               AS crm_account_owner_id,
      proposed_account_owner.user_id                                      AS proposed_crm_account_owner_id,
      technical_account_manager.user_id                                   AS technical_account_manager_id,
      sfdc_account.executive_sponsor_id,                                   
      sfdc_account.master_record_id,
      prep_crm_person.dim_crm_person_id                                   AS dim_crm_person_primary_contact_id,

      --account people
      account_owner.name                                                  AS account_owner,
      proposed_account_owner.name                                         AS proposed_crm_account_owner,
      technical_account_manager.name                                      AS technical_account_manager,

      -- account owner fields
      account_owner.user_segment                                          AS crm_account_owner_sales_segment,
      account_owner.user_geo                                              AS crm_account_owner_geo,
      account_owner.user_region                                           AS crm_account_owner_region,
      account_owner.user_area                                             AS crm_account_owner_area,
      account_owner.user_segment_geo_region_area                          AS crm_account_owner_sales_segment_geo_region_area,
      account_owner.title                                                 AS crm_account_owner_title,
      sfdc_user_roles_source.name                                         AS crm_account_owner_role,

      ----ultimate parent crm account info
       sfdc_account.ultimate_parent_account_name                          AS parent_crm_account_name,

      --technical account manager attributes
      technical_account_manager.manager_name AS tam_manager,

      --executive sponsor field
      executive_sponsor.name AS executive_sponsor,

      --6 sense fields
      sfdc_account.has_six_sense_6_qa,
      sfdc_account.risk_rate_guid,
      sfdc_account.six_sense_account_profile_fit,
      sfdc_account.six_sense_account_reach_score,
      sfdc_account.six_sense_account_profile_score,
      sfdc_account.six_sense_account_buying_stage,
      sfdc_account.six_sense_account_numerical_reach_score,
      sfdc_account.six_sense_account_update_date,
      sfdc_account.six_sense_account_6_qa_end_date,
      sfdc_account.six_sense_account_6_qa_age_days,
      sfdc_account.six_sense_account_6_qa_start_date,
      sfdc_account.six_sense_account_intent_score,
      sfdc_account.six_sense_segments, 

      --descriptive attributes
      sfdc_account.account_name                                           AS crm_account_name,
      sfdc_account.account_sales_segment                                  AS parent_crm_account_sales_segment,
      sfdc_account.account_geo                                            AS parent_crm_account_geo,
      sfdc_account.account_region                                         AS parent_crm_account_region,
      sfdc_account.account_area                                           AS parent_crm_account_area,
      sfdc_account.account_territory                                      AS parent_crm_account_territory,
      sfdc_account.account_business_unit                                  AS parent_crm_account_business_unit,
      sfdc_account.account_role_type                                      AS parent_crm_account_role_type,

      {%- if model_type == 'live' %}
        CONCAT(
               UPPER(account_owner_role),
               '-',
               current_fiscal_year.fiscal_year
               )                                                                                                                     AS dim_crm_parent_account_hierarchy_sk,
      {%- elif model_type == 'snapshot' %}
      CASE
        WHEN sfdc_account.snapshot_fiscal_year < 2024
          THEN CONCAT(
                      UPPER(parent_crm_account_sales_segment), 
                      '-',
                      UPPER(parent_crm_account_geo), 
                      '-',
                      UPPER(parent_crm_account_region), 
                      '-',
                      UPPER(parent_crm_account_area),
                      '-',
                      sfdc_account.snapshot_fiscal_year
                      )
        WHEN sfdc_account.snapshot_fiscal_year = 2024 AND LOWER(parent_crm_account_business_unit) = 'comm'
          THEN CONCAT(
                      UPPER(parent_crm_account_business_unit), 
                      '-',
                      UPPER(parent_crm_account_geo), 
                      '-',
                      UPPER(parent_crm_account_sales_segment), 
                      '-',
                      UPPER(parent_crm_account_region), 
                      '-',
                      UPPER(parent_crm_account_area),
                      '-',
                      sfdc_account.snapshot_fiscal_year
                      )
        WHEN sfdc_account.snapshot_fiscal_year = 2024 AND LOWER(parent_crm_account_business_unit) = 'entg'
          THEN CONCAT(
                      UPPER(parent_crm_account_business_unit), 
                      '-',
                      UPPER(parent_crm_account_geo), 
                      '-',
                      UPPER(parent_crm_account_region), 
                      '-',
                      UPPER(parent_crm_account_area), 
                      '-',
                      UPPER(parent_crm_account_sales_segment),
                      '-',
                      sfdc_account.snapshot_fiscal_year
                      )
        WHEN sfdc_account.snapshot_fiscal_year = 2024
          AND (parent_crm_account_business_unit IS NOT NULL AND LOWER(parent_crm_account_business_unit) NOT IN ('comm', 'entg'))  -- account for non-sales reps
          THEN CONCAT(
                      UPPER(parent_crm_account_business_unit), 
                      '-',
                      UPPER(parent_crm_account_sales_segment), 
                      '-',
                      UPPER(parent_crm_account_geo), 
                      '-',
                      UPPER(parent_crm_account_region), 
                      '-',
                      UPPER(parent_crm_account_area),
                      '-',
                      sfdc_account.snapshot_fiscal_year
                      )

        WHEN sfdc_account.snapshot_fiscal_year = 2024 AND parent_crm_account_business_unit IS NULL -- account for nulls/possible data issues
          THEN CONCAT(
                      UPPER(parent_crm_account_sales_segment), 
                      '-',
                      UPPER(parent_crm_account_geo), 
                      '-',
                      UPPER(parent_crm_account_region), 
                      '-',
                      UPPER(parent_crm_account_area),
                      '-',
                      sfdc_account.snapshot_fiscal_year
                      )
        WHEN sfdc_account.snapshot_fiscal_year >= 2025
          THEN CONCAT(
                      UPPER(account_owner_role),
                      '-',
                      sfdc_account.snapshot_fiscal_year
                      )  
        END                                                                                                                           AS dim_crm_parent_account_hierarchy_sk,
      {%- endif %}


      sfdc_account.account_max_family_employee                            AS parent_crm_account_max_family_employee,
      sfdc_account.account_upa_country                                    AS parent_crm_account_upa_country,
      sfdc_account.account_upa_state                                      AS parent_crm_account_upa_state,
      sfdc_account.account_upa_city                                       AS parent_crm_account_upa_city,
      sfdc_account.account_upa_street                                     AS parent_crm_account_upa_street,
      sfdc_account.account_upa_postal_code                                AS parent_crm_account_upa_postal_code,
      sfdc_account.account_employee_count                                 AS crm_account_employee_count,
      sfdc_account.parent_account_industry_hierarchy                      AS parent_crm_account_industry,
      sfdc_account.gtm_strategy                                           AS crm_account_gtm_strategy,
      CASE 
        WHEN sfdc_account.account_sales_segment IN ('Large', 'PubSec') THEN 'Large'
        WHEN sfdc_account.account_sales_segment = 'Unknown' THEN 'SMB'
        ELSE sfdc_account.account_sales_segment
      END                                                                 AS parent_crm_account_sales_segment_grouped,
      {{ sales_segment_region_grouped('sfdc_account.account_sales_segment',
        'sfdc_account.account_geo', 'sfdc_account.account_region') }} AS parent_crm_account_segment_region_stamped_grouped,
      CASE
        WHEN LOWER(sfdc_account.gtm_strategy) IN ('account centric', 'account based - net new', 'account based - expand') THEN 'Focus Account'
        ELSE 'Non - Focus Account'
      END                                                                 AS crm_account_focus_account,
      sfdc_account.account_owner_user_segment                             AS crm_account_owner_user_segment,
      sfdc_account.billing_country                                        AS crm_account_billing_country,
      sfdc_account.billing_country_code                                   AS crm_account_billing_country_code,
      sfdc_account.account_type                                           AS crm_account_type,
      sfdc_account.industry                                               AS crm_account_industry,
      sfdc_account.sub_industry                                           AS crm_account_sub_industry,
      sfdc_account.account_owner                                          AS crm_account_owner,
      CASE
         WHEN sfdc_account.account_max_family_employee > 2000 THEN 'Employees > 2K'
         WHEN sfdc_account.account_max_family_employee <= 2000 AND sfdc_account.account_max_family_employee > 1500 THEN 'Employees > 1.5K'
         WHEN sfdc_account.account_max_family_employee <= 1500 AND sfdc_account.account_max_family_employee > 1000  THEN 'Employees > 1K'
         ELSE 'Employees < 1K'
      END                                                                 AS crm_account_employee_count_band,
      sfdc_account.partner_vat_tax_id,
      sfdc_account.account_manager,
      sfdc_account.business_development_rep,
      sfdc_account.dedicated_service_engineer,
      sfdc_account.account_tier,
      sfdc_account.account_tier_notes,
      sfdc_account.license_utilization,
      sfdc_account.support_level,
      sfdc_account.named_account,
      sfdc_account.billing_postal_code,
      sfdc_account.partner_type,
      sfdc_account.partner_status,
      sfdc_account.gitlab_customer_success_project,
      sfdc_account.demandbase_account_list,
      sfdc_account.demandbase_intent,
      sfdc_account.demandbase_page_views,
      sfdc_account.demandbase_score,
      sfdc_account.demandbase_sessions,
      sfdc_account.demandbase_trending_offsite_intent,
      sfdc_account.demandbase_trending_onsite_engagement,
      sfdc_account.is_locally_managed_account,
      sfdc_account.is_strategic_account,
      sfdc_account.partner_track,
      sfdc_account.partners_partner_type,
      sfdc_account.gitlab_partner_program,
      sfdc_account.zoom_info_company_name,
      sfdc_account.zoom_info_company_revenue,
      sfdc_account.zoom_info_company_employee_count,
      sfdc_account.zoom_info_company_industry,
      sfdc_account.zoom_info_company_city,
      sfdc_account.zoom_info_company_state_province,
      sfdc_account.zoom_info_company_country,
      sfdc_account.account_phone,
      sfdc_account.zoominfo_account_phone,
      sfdc_account.abm_tier,
      sfdc_account.health_number,
      sfdc_account.health_score_color,
      sfdc_account.partner_account_iban_number,
      sfdc_account.gitlab_com_user,
      sfdc_account.zi_technologies                                        AS crm_account_zi_technologies,
      sfdc_account.zoom_info_website                                      AS crm_account_zoom_info_website,
      sfdc_account.zoom_info_company_other_domains                        AS crm_account_zoom_info_company_other_domains,
      sfdc_account.zoom_info_dozisf_zi_id                                 AS crm_account_zoom_info_dozisf_zi_id,
      sfdc_account.zoom_info_parent_company_zi_id                         AS crm_account_zoom_info_parent_company_zi_id,
      sfdc_account.zoom_info_parent_company_name                          AS crm_account_zoom_info_parent_company_name,
      sfdc_account.zoom_info_ultimate_parent_company_zi_id                AS crm_account_zoom_info_ultimate_parent_company_zi_id,
      sfdc_account.zoom_info_ultimate_parent_company_name                 AS crm_account_zoom_info_ultimate_parent_company_name,
      sfdc_account.zoom_info_number_of_developers                         AS crm_account_zoom_info_number_of_developers,
      sfdc_account.zoom_info_total_funding                                AS crm_account_zoom_info_total_funding,
      sfdc_account.forbes_2000_rank,
      sfdc_account.parent_account_industry_hierarchy,
      sfdc_account.crm_sales_dev_rep_id,
      sfdc_account.admin_manual_source_number_of_employees,
      sfdc_account.admin_manual_source_account_address,
      sfdc_account.eoa_sentiment,
      sfdc_account.gs_health_user_engagement,
      sfdc_account.gs_health_cd,
      sfdc_account.gs_health_devsecops,
      sfdc_account.gs_health_ci,
      sfdc_account.gs_health_scm,
      sfdc_account.risk_impact,
      sfdc_account.risk_reason,
      sfdc_account.last_timeline_at_risk_update,
      sfdc_account.last_at_risk_update_comments,
      sfdc_account.bdr_prospecting_status,
      sfdc_account.gs_health_csm_sentiment,

      --degenerative dimensions
      sfdc_account.is_sdr_target_account,
      sfdc_account.is_focus_partner,
      IFF(sfdc_record_type.record_type_label = 'Partner'
          AND sfdc_account.partner_type IN ('Alliance', 'Channel')
          AND sfdc_account.partner_status = 'Authorized',
          TRUE, FALSE)                                                    AS is_reseller,
      sfdc_account.is_jihu_account                                        AS is_jihu_account,
      sfdc_account.is_first_order_available,
      sfdc_account.is_key_account                                         AS is_key_account,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies,'ARE_USED: Jenkins')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_jenkins_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: SVN')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_svn_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Tortoise SVN')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_tortoise_svn_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Google Cloud Platform')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_gcp_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Atlassian')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_atlassian_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: GitHub')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_github_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: GitHub Enterprise')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_github_enterprise_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: AWS')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_aws_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Kubernetes')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_kubernetes_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Apache Subversion')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_apache_subversion_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Apache Subversion (SVN)')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_apache_subversion_svn_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Hashicorp')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_hashicorp_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: Amazon AWS CloudTrail')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_aws_cloud_trail_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: CircleCI')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_circle_ci_present,
      CASE
        WHEN CONTAINS (sfdc_account.zi_technologies, 'ARE_USED: BitBucket')
          THEN 1
        ELSE 0
      END                                                                 AS is_zi_bit_bucket_present,
      sfdc_account.is_excluded_from_zoom_info_enrich,

      --dates
      {{ get_date_id('sfdc_account.created_date') }}                      AS crm_account_created_date_id,
      sfdc_account.created_date                                           AS crm_account_created_date,
      {{ get_date_id('sfdc_account.abm_tier_1_date') }}                   AS abm_tier_1_date_id,
      sfdc_account.abm_tier_1_date,
      {{ get_date_id('sfdc_account.abm_tier_2_date') }}                   AS abm_tier_2_date_id,
      sfdc_account.abm_tier_2_date,
      {{ get_date_id('sfdc_account.abm_tier_3_date') }}                   AS abm_tier_3_date_id,
      sfdc_account.abm_tier_3_date,
      {{ get_date_id('sfdc_account.gtm_acceleration_date') }}             AS gtm_acceleration_date_id,
      sfdc_account.gtm_acceleration_date,
      {{ get_date_id('sfdc_account.gtm_account_based_date') }}            AS gtm_account_based_date_id,
      sfdc_account.gtm_account_based_date,
      {{ get_date_id('sfdc_account.gtm_account_centric_date') }}          AS gtm_account_centric_date_id,
      sfdc_account.gtm_account_centric_date,
      {{ get_date_id('sfdc_account.partners_signed_contract_date') }}     AS partners_signed_contract_date_id,
      CAST(sfdc_account.partners_signed_contract_date AS date)            AS partners_signed_contract_date,
      {{ get_date_id('sfdc_account.technical_account_manager_date') }}    AS technical_account_manager_date_id,
      sfdc_account.technical_account_manager_date,
      {{ get_date_id('sfdc_account.customer_since_date') }}               AS customer_since_date_id,
      sfdc_account.customer_since_date,
      {{ get_date_id('sfdc_account.next_renewal_date') }}                 AS next_renewal_date_id,
      sfdc_account.next_renewal_date,
      {{ get_date_id('sfdc_account.gs_first_value_date') }}               AS gs_first_value_date_id,
      sfdc_account.gs_first_value_date,
      {{ get_date_id('sfdc_account.gs_last_csm_activity_date') }}         AS gs_last_csm_activity_date_id,
      sfdc_account.gs_last_csm_activity_date,


      --measures
      sfdc_account.count_active_subscription_charges,
      sfdc_account.count_active_subscriptions,
      sfdc_account.count_billing_accounts,
      sfdc_account.count_licensed_users,
      sfdc_account.count_of_new_business_won_opportunities,
      sfdc_account.count_open_renewal_opportunities,
      sfdc_account.count_opportunities,
      sfdc_account.count_products_purchased,
      sfdc_account.count_won_opportunities,
      sfdc_account.count_concurrent_ee_subscriptions,
      sfdc_account.count_ce_instances,
      sfdc_account.count_active_ce_users,
      sfdc_account.count_open_opportunities,
      sfdc_account.count_using_ce,
      sfdc_account.carr_this_account,
      sfdc_account.carr_account_family,
      sfdc_account.potential_users,
      sfdc_account.number_of_licenses_this_account,
      sfdc_account.decision_maker_count_linkedin,
      sfdc_account.number_of_employees,
      crm_user.user_role_type                                             AS user_role_type,
      crm_user.user_role_name                                             AS owner_role,
      {%- if model_type == 'live' %}
      sfdc_account.lam                                                    AS parent_crm_account_lam,
      sfdc_account.lam_dev_count                                          AS parent_crm_account_lam_dev_count,
      {%- elif model_type == 'snapshot' %}
      IFNULL(lam_corrections.estimated_capped_lam, sfdc_account.lam)      AS parent_crm_account_lam,
      IFNULL(lam_corrections.dev_count, sfdc_account.lam_dev_count)       AS parent_crm_account_lam_dev_count,
      {%- endif %}

      -- PtC and PtE 
      pte_scores.score                                               AS pte_score,
      pte_scores.decile                                              AS pte_decile,
      pte_scores.score_group                                         AS pte_score_group,
      ptc_scores.score                                               AS ptc_score,
      ptc_scores.decile                                              AS ptc_decile,
      ptc_scores.score_group                                         AS ptc_score_group,


      --metadata
      sfdc_account.created_by_id,
      created_by.name                                                     AS created_by_name,
      sfdc_account.last_modified_by_id,
      last_modified_by.name                                               AS last_modified_by_name,
      {{ get_date_id('sfdc_account.last_modified_date') }}                AS last_modified_date_id,
      sfdc_account.last_modified_date,
      {{ get_date_id('sfdc_account.last_activity_date') }}                AS last_activity_date_id,
      sfdc_account.last_activity_date,
      sfdc_account.is_deleted

    FROM sfdc_account
    LEFT JOIN map_merged_crm_account
      ON sfdc_account.account_id = map_merged_crm_account.sfdc_account_id
    LEFT JOIN sfdc_record_type
      ON sfdc_account.record_type_id = sfdc_record_type.record_type_id
    LEFT JOIN prep_crm_person
      ON sfdc_account.primary_contact_id = prep_crm_person.sfdc_record_id
    {%- if model_type == 'live' %}
    LEFT JOIN pte_scores 
      ON sfdc_account.account_id = pte_scores.account_id 
        AND pte_scores.is_current = TRUE
    LEFT JOIN ptc_scores
      ON sfdc_account.account_id = ptc_scores.account_id 
        AND ptc_scores.is_current = TRUE
    LEFT OUTER JOIN sfdc_users AS technical_account_manager
      ON sfdc_account.technical_account_manager_id = technical_account_manager.user_id
    LEFT JOIN sfdc_users AS account_owner
      ON sfdc_account.owner_id = account_owner.user_id
    LEFT JOIN sfdc_users AS proposed_account_owner
      ON proposed_account_owner.user_id = sfdc_account.proposed_account_owner
    LEFT JOIN sfdc_users AS executive_sponsor
      ON executive_sponsor.user_id = sfdc_account.executive_sponsor_id
    LEFT JOIN sfdc_users created_by
      ON sfdc_account.created_by_id = created_by.user_id
    LEFT JOIN sfdc_users AS last_modified_by
      ON sfdc_account.last_modified_by_id = last_modified_by.user_id
    LEFT JOIN crm_user
      ON sfdc_account.owner_id = crm_user.dim_crm_user_id
    {%- elif model_type == 'snapshot' %}
    LEFT OUTER JOIN sfdc_users AS technical_account_manager
      ON sfdc_account.technical_account_manager_id = technical_account_manager.user_id
        AND sfdc_account.snapshot_id = technical_account_manager.snapshot_id
    LEFT JOIN sfdc_users AS account_owner
      ON account_owner.user_id = sfdc_account.owner_id
        AND account_owner.snapshot_id = sfdc_account.snapshot_id
    LEFT JOIN sfdc_users AS proposed_account_owner
      ON proposed_account_owner.user_id = sfdc_account.proposed_account_owner
        AND proposed_account_owner.snapshot_id = sfdc_account.snapshot_id
    LEFT JOIN sfdc_users AS executive_sponsor
      ON executive_sponsor.user_id = sfdc_account.executive_sponsor_id
        AND executive_sponsor.snapshot_id = sfdc_account.snapshot_id
    LEFT JOIN lam_corrections
      ON sfdc_account.ultimate_parent_account_id = lam_corrections.dim_parent_crm_account_id
        AND sfdc_account.snapshot_id = lam_corrections.snapshot_id
        AND sfdc_account.account_sales_segment = lam_corrections.parent_crm_account_sales_segment
    LEFT JOIN sfdc_users AS created_by
      ON sfdc_account.created_by_id = created_by.user_id
        AND sfdc_account.snapshot_id = created_by.snapshot_id
    LEFT JOIN sfdc_users AS last_modified_by
      ON sfdc_account.last_modified_by_id = last_modified_by.user_id
        AND sfdc_account.snapshot_id = last_modified_by.snapshot_id
    LEFT JOIN pte_scores 
      ON sfdc_account.account_id = pte_scores.account_id
        AND sfdc_account.snapshot_date >= pte_scores.valid_from::DATE
        AND  sfdc_account.snapshot_date < pte_scores.valid_to::DATE
    LEFT JOIN ptc_scores 
      ON sfdc_account.account_id = ptc_scores.account_id
        AND sfdc_account.snapshot_date >= ptc_scores.valid_from::DATE
        AND  sfdc_account.snapshot_date < ptc_scores.valid_to::DATE
    LEFT JOIN crm_user
      ON sfdc_account.owner_id = crm_user.dim_crm_user_id
        AND sfdc_account.snapshot_id = crm_user.snapshot_id
    {%- endif %}
     LEFT JOIN sfdc_user_roles_source
      ON account_owner.user_role_id = sfdc_user_roles_source.id
     LEFT JOIN current_fiscal_year


)

{%- endmacro %}
