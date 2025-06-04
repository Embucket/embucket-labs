{{ config(
    tags=["mnpi", "six_hourly"]
) }}

/*
  This model pulls all fields from the `sfdc_opportunity_snapshots_source` table,
  so any new fields added to that table will automatically be included here.
  Avoid adding fields directly to this model unless a specific transformation is required.

  Fields can be added to the exclude list in the `star` macro to prevent it from being pulled in.
*/

WITH source AS (

  SELECT *
  FROM {{ ref('sfdc_opportunity_snapshots_source') }}

),

updated_source_with_net_arr_logic AS (

  SELECT
    *,

    -- When net_arr exceeds 30M, the value from previous row is returned
    LAG(net_arr) OVER (
      PARTITION BY opportunity_id
      ORDER BY dbt_valid_from ASC)                                                                                  AS net_arr_lag_logic,
    CASE
      WHEN net_arr >= 30000000 AND net_arr_lag_logic < 30000000 
        THEN net_arr_lag_logic
      WHEN net_arr >= 30000000 
        THEN NULL
      ELSE net_arr
    END                                                                                                             AS net_arr_updated
  FROM source

),

calculations AS (

  SELECT
    -- Pull all fields from the source table
    {{ dbt_utils.star(
        from=ref('sfdc_opportunity_snapshots_source'), 
        except=['created_date', 'sales_accepted_date', 'close_date', 'deal_path', 'order_type_stamped', 'opportunity_id', 'owner_id', 'account_id', 'parent_opportunity_id', 'net_arr', 'opportunity_term', 'is_won', 'sdc_batched_at', 'sdc_received_at', 'sdc_sequence', 'sdc_extracted_at', 'ultimate_parent_sales_segment_emp', 'sales_segmentation_employees'],
      ) 
    }},
    CASE
      WHEN is_web_portal_purchase THEN 'Web Direct'
      WHEN net_arr_updated < 5000 THEN '<5K'
      WHEN net_arr_updated < 25000 THEN '5-25K'
      WHEN net_arr_updated < 100000 THEN '25-100K'
      WHEN net_arr_updated < 250000 THEN '100-250K'
      WHEN net_arr_updated > 250000 THEN '250K+'
      ELSE 'Missing opportunity_deal_size'
    END                                                                                                             AS opportunity_deal_size,
    -- dates & date ids
    {{ get_date_id('updated_source_with_net_arr_logic.iacv_created_date') }}                                        AS arr_created_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.created_date') }}                                             AS created_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.sales_accepted_date') }}                                      AS sales_accepted_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.close_date') }}                                               AS close_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.stage_0_pending_acceptance_date') }}                          AS stage_0_pending_acceptance_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.stage_1_discovery_date') }}                                   AS stage_1_discovery_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.stage_2_scoping_date') }}                                     AS stage_2_scoping_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.stage_3_technical_evaluation_date') }}                        AS stage_3_technical_evaluation_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.stage_4_proposal_date') }}                                    AS stage_4_proposal_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.stage_5_negotiating_date') }}                                 AS stage_5_negotiating_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.stage_6_awaiting_signature_date') }}                          AS stage_6_awaiting_signature_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.stage_6_closed_won_date') }}                                  AS stage_6_closed_won_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.stage_6_closed_lost_date') }}                                 AS stage_6_closed_lost_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.technical_evaluation_date') }}                                AS technical_evaluation_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.last_activity_date') }}                                       AS last_activity_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.sales_last_activity_date') }}                                 AS sales_last_activity_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.subscription_start_date') }}                                  AS subscription_start_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.subscription_end_date') }}                                    AS subscription_end_date_id,
    {{ get_date_id('updated_source_with_net_arr_logic.sales_qualified_date') }}                                     AS sales_qualified_date_id,

    -- Opportunity fields
    updated_source_with_net_arr_logic.opportunity_id                                                                AS dim_crm_opportunity_id,
    {{ get_keyed_nulls('updated_source_with_net_arr_logic.owner_id') }}                                             AS dim_crm_user_id,
    updated_source_with_net_arr_logic.account_id                                                                    AS dim_crm_account_id,
    updated_source_with_net_arr_logic.parent_opportunity_id                                                         AS dim_parent_crm_opportunity_id,
    updated_source_with_net_arr_logic.iacv_created_date                                                             AS arr_created_date,
    updated_source_with_net_arr_logic.user_geo_stamped                                                              AS crm_opp_owner_geo_stamped,
    updated_source_with_net_arr_logic.user_region_stamped                                                           AS crm_opp_owner_region_stamped,
    updated_source_with_net_arr_logic.user_area_stamped                                                             AS crm_opp_owner_area_stamped,
    updated_source_with_net_arr_logic.user_business_unit_stamped                                                    AS crm_opp_owner_business_unit_stamped,
    updated_source_with_net_arr_logic.net_arr_updated                                                               AS raw_net_arr,
    updated_source_with_net_arr_logic.order_type_stamped                                                            AS order_type,
    updated_source_with_net_arr_logic.opportunity_term                                                              AS opportunity_term_base,

    -- Timestamp and Dates
    DATEDIFF(DAYS, last_activity_date::DATE, CURRENT_DATE)                                                          AS days_since_last_activity,
    updated_source_with_net_arr_logic.created_date::DATE                                                            AS created_date,
    updated_source_with_net_arr_logic.sales_accepted_date::DATE                                                     AS sales_accepted_date,
    updated_source_with_net_arr_logic.close_date::DATE                                                              AS close_date,
    CASE
      WHEN stage_name = '0-Pending Acceptance' THEN DATEDIFF(DAYS, stage_0_pending_acceptance_date::DATE, CURRENT_DATE::DATE) + 1
      WHEN stage_name = '1-Discovery' THEN DATEDIFF(DAYS, stage_1_discovery_date::DATE, CURRENT_DATE::DATE) + 1
      WHEN stage_name = '2-Scoping' THEN DATEDIFF(DAYS, stage_2_scoping_date::DATE, CURRENT_DATE::DATE) + 1
      WHEN stage_name = '3-Technical Evaluation' THEN DATEDIFF(DAYS, stage_3_technical_evaluation_date::DATE, CURRENT_DATE::DATE) + 1
      WHEN stage_name = '4-Proposal' THEN DATEDIFF(DAYS, stage_4_proposal_date::DATE, CURRENT_DATE::DATE) + 1
      WHEN stage_name = '5-Negotiating' THEN DATEDIFF(DAYS, stage_5_negotiating_date::DATE, CURRENT_DATE::DATE) + 1
      WHEN stage_name = '6-Awaiting Signature' THEN DATEDIFF(DAYS, stage_6_awaiting_signature_date::DATE, CURRENT_DATE::DATE) + 1
    END                                                                                                             AS days_in_stage,
    -- The fiscal year has to be created from scratch instead of joining to the date model because of sales practices which put close dates out 100+ years in the future
    CASE
      WHEN DATE_PART('month', updated_source_with_net_arr_logic.close_date) < 2
        THEN DATE_PART('year', updated_source_with_net_arr_logic.close_date)
      ELSE (DATE_PART('year', updated_source_with_net_arr_logic.close_date) + 1)
    END                                                                                                             AS close_fiscal_year,

    -- Opportunity Details
    {{ sfdc_source_buckets('lead_source') }}
    {{ deal_path_cleaning('deal_path') }}                                                                           AS deal_path,
    {{ sales_qualified_source_cleaning('generated_source') }}                                                       AS sales_qualified_source,
    {{ sales_qualified_source_grouped('sales_qualified_source') }}                                                  AS sales_qualified_source_grouped,
    {{ sqs_bucket_engagement('sales_qualified_source') }}                                                           AS sqs_bucket_engagement,

    -- Deal Metrics and Growth
    CASE
      WHEN order_type_stamped = '1. New - First Order'
        THEN '1) New - First Order'
      WHEN order_type_stamped IN ('2. New - Connected', '3. Growth', '4. Contraction', '5. Churn - Partial', '6. Churn - Final')
        THEN '2) Growth (Growth / New - Connected / Churn / Contraction)'
      WHEN order_type_stamped IN ('7. PS / Other')
        THEN '3) Consumption / PS / Other'
      ELSE 'Missing order_type_name_grouped'
    END                                                                                                             AS order_type_grouped,
    {{ growth_type('order_type_stamped', 'arr_basis') }}                                                            AS growth_type,

    -- Hierarchy
    {{ sales_hierarchy_sales_segment_cleaning('user_segment') }}                                                    AS user_segment_stamped,
    {{ sales_segment_region_grouped('user_segment_stamped', 'user_geo_stamped', 'user_region_stamped') }}
      AS user_segment_region_stamped_grouped,
    CONCAT(
      user_segment_stamped,
      '-',
      user_geo_stamped,
      '-',
      user_region_stamped,
      '-',
      user_area_stamped
    )                                                                                                               AS user_segment_geo_region_area_stamped,
    {{ sales_hierarchy_sales_segment_cleaning('user_segment') }}                                                    AS crm_opp_owner_sales_segment_stamped,
    CASE
      WHEN crm_opp_owner_sales_segment_stamped IN ('Large', 'PubSec') THEN 'Large'
      ELSE crm_opp_owner_sales_segment_stamped
    END                                                                                                             AS user_segment_stamped_grouped,
    CONCAT(
      crm_opp_owner_sales_segment_stamped,
      '-',
      user_geo_stamped,
      '-',
      user_region_stamped,
      '-',
      user_area_stamped
    )                                                                                                               AS crm_opp_owner_sales_segment_geo_region_area_stamped,
    CASE
      WHEN sao_crm_opp_owner_sales_segment_stamped IN ('Large', 'PubSec') THEN 'Large'
      ELSE sao_crm_opp_owner_sales_segment_stamped
    END                                                                                                             AS sao_crm_opp_owner_sales_segment_stamped_grouped,
    {{ sales_segment_region_grouped('sao_crm_opp_owner_sales_segment_stamped', 'sao_crm_opp_owner_geo_stamped', 'sao_crm_opp_owner_region_stamped') }}
      AS sao_crm_opp_owner_segment_region_stamped_grouped,
    COALESCE({{ sales_segment_cleaning('sales_segmentation_employees') }}, 'Unknown')                               AS sales_segment,
    {{ sales_segment_cleaning('ultimate_parent_sales_segment_emp') }}                                               AS parent_segment,
    COALESCE({{ sales_segment_cleaning('sales_segmentation_employees') }}, 'Unknown')                               AS division_sales_segment_stamped,
    {{ channel_type('sqs_bucket_engagement', 'order_type_stamped') }}                                               AS channel_type,
    CASE
      WHEN stage_name IN (
          '8-Closed Lost', 'Closed Lost', '9-Unqualified',
          'Closed Won', '10-Duplicate'
        )
        THEN 0
      ELSE 1
    END                                                                                                             AS is_open,
    COALESCE((
      days_in_stage > 30
      OR updated_source_with_net_arr_logic.incremental_acv > 100000
      OR updated_source_with_net_arr_logic.pushed_count > 0
    ), FALSE)                                                                                                       AS is_risky,
    IFF(updated_source_with_net_arr_logic.stage_name IN (
      '1-Discovery', '2-Developing', '2-Scoping',
      '3-Technical Evaluation', '4-Proposal', 'Closed Won',
      '5-Negotiating', '6-Awaiting Signature', '7-Closing'
    ), 1, 0)                                                                                                        AS is_stage_1_plus,
    IFF(updated_source_with_net_arr_logic.stage_name IN (
      '3-Technical Evaluation', '4-Proposal', 'Closed Won',
      '5-Negotiating', '6-Awaiting Signature', '7-Closing'
    ), 1, 0)                                                                                                        AS is_stage_3_plus,
    IFF(updated_source_with_net_arr_logic.stage_name IN (
      '4-Proposal', 'Closed Won', '5-Negotiating',
      '6-Awaiting Signature', '7-Closing'
    ), 1, 0)                                                                                                        AS is_stage_4_plus,
    IFF(updated_source_with_net_arr_logic.stage_name IN ('8-Closed Lost', 'Closed Lost'), 1, 0)                     AS is_lost,
    IFF(LOWER(updated_source_with_net_arr_logic.subscription_type) LIKE '%renewal%', 1, 0)                          AS is_renewal,
    CASE
      WHEN updated_source_with_net_arr_logic.stage_name IN ('10-Duplicate') THEN 1
      ELSE 0
    END                                                                                                             AS is_duplicate,
    IFF(updated_source_with_net_arr_logic.comp_new_logo_override = 'Yes', 1, 0)                                     AS is_comp_new_logo_override,
    IFF(acv >= 0, 1, 0)                                                                                             AS is_closed_deals,
    CASE
      WHEN updated_source_with_net_arr_logic.days_in_sao < 0 THEN '1. Closed in < 0 days'
      WHEN updated_source_with_net_arr_logic.days_in_sao BETWEEN 0 AND 30 THEN '2. Closed in 0-30 days'
      WHEN updated_source_with_net_arr_logic.days_in_sao BETWEEN 31 AND 60 THEN '3. Closed in 31-60 days'
      WHEN updated_source_with_net_arr_logic.days_in_sao BETWEEN 61 AND 90 THEN '4. Closed in 61-90 days'
      WHEN updated_source_with_net_arr_logic.days_in_sao BETWEEN 91 AND 180 THEN '5. Closed in 91-180 days'
      WHEN updated_source_with_net_arr_logic.days_in_sao BETWEEN 181 AND 270 THEN '6. Closed in 181-270 days'
      WHEN updated_source_with_net_arr_logic.days_in_sao > 270 THEN '7. Closed in > 270 days'
    END                                                                                                             AS closed_buckets,
    CASE
      WHEN updated_source_with_net_arr_logic.created_date < '2022-02-01' THEN 'Legacy'
      WHEN updated_source_with_net_arr_logic.crm_sales_dev_rep_id IS NOT NULL
        AND updated_source_with_net_arr_logic.crm_business_dev_rep_id IS NOT NULL THEN 'SDR & BDR'
      WHEN updated_source_with_net_arr_logic.crm_sales_dev_rep_id IS NOT NULL THEN 'SDR'
      WHEN updated_source_with_net_arr_logic.crm_business_dev_rep_id IS NOT NULL THEN 'BDR'
      ELSE 'No XDR Assigned'
    END                                                                                                             AS sdr_or_bdr,
    COALESCE(updated_source_with_net_arr_logic.reason_for_loss, updated_source_with_net_arr_logic.downgrade_reason) AS reason_for_loss_staged,
    CASE
      WHEN reason_for_loss_staged IN ('Do Nothing', 'Other', 'Competitive Loss', 'Operational Silos')
        OR reason_for_loss_staged IS NULL
        THEN 'Unknown'
      WHEN reason_for_loss_staged IN (
          'Missing Feature', 'Product value/gaps', 'Product Value / Gaps',
          'Stayed with Community Edition', 'Budget/Value Unperceived'
        )
        THEN 'Product Value / Gaps'
      WHEN reason_for_loss_staged IN ('Lack of Engagement / Sponsor', 'Went Silent', 'Evangelist Left')
        THEN 'Lack of Engagement / Sponsor'
      WHEN reason_for_loss_staged IN ('Loss of Budget', 'No budget')
        THEN 'Loss of Budget'
      WHEN reason_for_loss_staged = 'Merged into another opportunity'
        THEN 'Merged Opp'
      WHEN reason_for_loss_staged = 'Stale Opportunity'
        THEN 'No Progression - Auto-close'
      WHEN reason_for_loss_staged IN ('Product Quality / Availability', 'Product quality/availability')
        THEN 'Product Quality / Availability'
      ELSE reason_for_loss_staged
    END                                                                                                             AS reason_for_loss_calc,

    -- Stage Name Calculations
    CASE
      WHEN updated_source_with_net_arr_logic.stage_name IN (
          '00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying',
          'Developing', '1-Discovery', '2-Developing', '2-Scoping'
        )
        THEN 'Pipeline'
      WHEN updated_source_with_net_arr_logic.stage_name IN (
          '3-Technical Evaluation', '4-Proposal', '5-Negotiating',
          '6-Awaiting Signature', '7-Closing'
        )
        THEN '3+ Pipeline'
      WHEN updated_source_with_net_arr_logic.stage_name IN ('8-Closed Lost', 'Closed Lost')
        THEN 'Lost'
      WHEN updated_source_with_net_arr_logic.stage_name IN ('Closed Won')
        THEN 'Closed Won'
      ELSE 'Other'
    END                                                                                                             AS stage_name_3plus,

    CASE
      WHEN updated_source_with_net_arr_logic.stage_name IN (
          '00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying',
          'Developing', '1-Discovery', '2-Developing', '2-Scoping',
          '3-Technical Evaluation'
        )
        THEN 'Pipeline'
      WHEN updated_source_with_net_arr_logic.stage_name IN (
          '4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing'
        )
        THEN '4+ Pipeline'
      WHEN updated_source_with_net_arr_logic.stage_name IN ('8-Closed Lost', 'Closed Lost')
        THEN 'Lost'
      WHEN updated_source_with_net_arr_logic.stage_name IN ('Closed Won')
        THEN 'Closed Won'
      ELSE 'Other'
    END                                                                                                             AS stage_name_4plus,

    -- Competitors
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'Other'), 1, 0)                                     AS competitors_other_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'GitLab Core'), 1, 0)                               AS competitors_gitlab_core_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'None'), 1, 0)                                      AS competitors_none_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'GitHub Enterprise'), 1, 0)                         AS competitors_github_enterprise_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'BitBucket Server'), 1, 0)                          AS competitors_bitbucket_server_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'Unknown'), 1, 0)                                   AS competitors_unknown_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'GitHub.com'), 1, 0)                                AS competitors_github_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'GitLab.com'), 1, 0)                                AS competitors_gitlab_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'Jenkins'), 1, 0)                                   AS competitors_jenkins_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'Azure DevOps'), 1, 0)                              AS competitors_azure_devops_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'SVN'), 1, 0)                                       AS competitors_svn_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'BitBucket.Org'), 1, 0)                             AS competitors_bitbucket_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'Atlassian'), 1, 0)                                 AS competitors_atlassian_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'Perforce'), 1, 0)                                  AS competitors_perforce_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'Visual Studio Team Services'), 1, 0)               AS competitors_visual_studio_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'Azure'), 1, 0)                                     AS competitors_azure_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'Amazon Code Commit'), 1, 0)                        AS competitors_amazon_code_commit_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'CircleCI'), 1, 0)                                  AS competitors_circleci_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'Bamboo'), 1, 0)                                    AS competitors_bamboo_flag,
    IFF(CONTAINS(updated_source_with_net_arr_logic.competitors, 'AWS'), 1, 0)                                       AS competitors_aws_flag,

    --metadata 
    CONVERT_TIMEZONE('America/Los_Angeles', CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))                           AS _last_dbt_run
  FROM updated_source_with_net_arr_logic

)

SELECT *
FROM calculations
