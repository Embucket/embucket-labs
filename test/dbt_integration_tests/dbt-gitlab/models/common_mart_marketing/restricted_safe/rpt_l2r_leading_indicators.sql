{{ config(
    materialized="table"
) }}

{{ simple_cte([
    ('rpt_lead_to_revenue','rpt_lead_to_revenue'),
    ('dim_date','dim_date')
]) }}

, rpt_lead_to_revenue_base AS ( 

    SELECT
    --IDs    
        dim_crm_person_id,
        dim_crm_opportunity_id,

    --Person Data
        email_hash,
        email_domain_type,
        person_order_type,
        account_demographics_sales_segment,
        account_demographics_geo,
        lead_source,
        inquiry_sum,
        mql_sum,
        lead_score_classification,
        is_defaulted_trial,

    --Person Dates
        true_inquiry_date_pt,
        mql_date_first_pt,
        mql_date_latest_pt,

    --Opportunity Data
        opp_order_type,
        report_segment,
        report_geo,
        sales_qualified_source_name,

    --Opportunity Dates
        sales_accepted_date,

    --Account Data
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        
    --Bizible Fields
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,

    --Flags
        is_mql,
        is_sao
    FROM rpt_lead_to_revenue
    WHERE (account_demographics_geo != 'JIHU'
        OR account_demographics_geo IS NULL) 
     AND (report_geo != 'JIHU'
        OR report_geo IS NULL)

), date_base AS (

    SELECT
        date_day,
        fiscal_year                     AS date_range_year,
        fiscal_quarter_name_fy          AS date_range_quarter,
        first_day_of_month              AS date_range_month,
        first_day_of_week               AS date_range_week
    FROM dim_date

), inquiry_prep AS (

    SELECT
        date_base.*,
        true_inquiry_date_pt,
        CASE 
            WHEN true_inquiry_date_pt IS NOT NULL 
                THEN email_hash
            ELSE NULL
        END AS actual_inquiry,
        email_domain_type,
        person_order_type,
        account_demographics_sales_segment,
        account_demographics_geo,
        lead_source,
        inquiry_sum,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count
    FROM rpt_lead_to_revenue_base
    LEFT JOIN date_base
        ON rpt_lead_to_revenue_base.true_inquiry_date_pt=date_base.date_day    
    WHERE 1=1
    AND (account_demographics_geo != 'JIHU'
        OR account_demographics_geo IS NULL)

 ), mql_prep AS (
     
    SELECT
        date_base.*,
        is_mql,
        CASE 
        WHEN is_mql = TRUE THEN email_hash
        ELSE NULL
        END AS mqls,
        email_domain_type,
        person_order_type,
        account_demographics_sales_segment,
        account_demographics_geo,
        lead_source,
        mql_sum,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count
  FROM rpt_lead_to_revenue_base
  LEFT JOIN date_base
    ON rpt_lead_to_revenue_base.mql_date_latest_pt=date_base.date_day
  WHERE 1=1 
   AND (account_demographics_geo != 'JIHU'
     OR account_demographics_geo IS NULL) 
  
), sao_prep AS (
     
    SELECT
        date_base.*,
        is_sao,
        opp_order_type,
        CASE 
            WHEN report_segment = 'LARGE' 
                THEN 'Large'
            WHEN report_segment = 'MID-MARKET' 
                THEN 'Mid-Market'
            WHEN report_segment = 'PUBSEC' 
                THEN 'PubSec'
            WHEN report_segment = 'OTHER' 
                THEN 'Other'
            ELSE report_segment
        END AS report_segment, 
        report_geo,
        sales_qualified_source_name,
        CASE 
            WHEN is_sao = TRUE 
                THEN dim_crm_opportunity_id 
            ELSE NULL 
        END AS saos,
        sales_accepted_date,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium
    FROM rpt_lead_to_revenue_base
    LEFT JOIN date_base 
        ON rpt_lead_to_revenue_base.sales_accepted_date=date_base.date_day
    WHERE 1=1
        AND sales_accepted_date <= CURRENT_DATE
        AND (report_geo != 'JIHU'
        OR report_geo IS NULL)

), inquiries AS (

    SELECT
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        person_order_type as order_type,
        account_demographics_sales_segment AS sales_segment,
        account_demographics_geo AS geo,
        lead_source,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        COUNT(DISTINCT actual_inquiry) AS inquiries
    FROM inquiry_prep
    {{ dbt_utils.group_by(n=14) }}
  
), mqls AS (

    SELECT
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        person_order_type as order_type,
        account_demographics_sales_segment AS sales_segment,
        account_demographics_geo AS geo,
        lead_source,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        COUNT(DISTINCT mqls) AS mqls
    FROM mql_prep
    {{ dbt_utils.group_by(n=14) }}
    
 ), saos AS (
  
    SELECT
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        report_segment AS sales_segment, 
        report_geo AS geo,
        sales_qualified_source_name,
        opp_order_type AS order_type,
        sales_accepted_date,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        COUNT(DISTINCT saos) AS saos
    FROM sao_prep
    {{ dbt_utils.group_by(n=15) }}
    
  ), intermediate AS (

    SELECT 
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        sales_segment,
        geo,
        order_type,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        SUM(inquiries) AS inquiries,
        0 AS mqls,
        0 AS saos
    FROM inquiries
    {{ dbt_utils.group_by(n=13) }}
    UNION ALL
    SELECT
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        sales_segment,
        geo,
        order_type,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        0 AS inquiries,
        SUM(mqls) AS mqls,
        0 AS saos
    FROM mqls
    {{ dbt_utils.group_by(n=13) }}
    UNION ALL
    SELECT
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        sales_segment,
        geo,
        order_type,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        0 AS inquiries,
        0 AS mqls,
        SUM(saos) AS saos
    FROM saos
    {{ dbt_utils.group_by(n=13) }}

  ), final AS (

    SELECT
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        sales_segment,
        geo,
        order_type,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        SUM(inquiries) AS inquiries,
        SUM(mqls) AS mqls,
        SUM(saos) AS saos
    FROM intermediate
    {{ dbt_utils.group_by(n=13) }}
  )


{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-06-21",
    updated_date="2024-10-07",
  ) }}