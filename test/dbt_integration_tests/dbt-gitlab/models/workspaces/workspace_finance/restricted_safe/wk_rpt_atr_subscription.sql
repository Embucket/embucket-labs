{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

{{ simple_cte([
    ('dim_subscription', 'dim_subscription'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('fct_available_to_renew_snapshot_model', 'fct_available_to_renew_snapshot_model'),
    ('dim_product_detail', 'dim_product_detail'),
    ('dim_date', 'dim_date'),
    ('sheetload_map_ramp_deals', 'sheetload_map_ramp_deals')

]) }},

-- Day 5 ATR Detail (Subscription) with the last_in_lineage field showing the latest subscription in lineage (including legacy ramps, change of entity, contract resets, late renewal, debook and rebook)

-- PART 1 of the new mart - identifying the standalone subscriptions that have been renewed based on the zuora_renewal_subscription_name and ramps through the sheetload

ramp_deals AS (

    SELECT 
        dim_crm_opportunity_id,
        "Overwrite_Term"
    FROM sheetload_map_ramp_deals
    WHERE "Overwrite_Term" IS NOT NULL
    
), opportunity_term_prep AS (

    SELECT 
        mart_crm_opportunity.dim_crm_opportunity_id,
        CASE 
            WHEN ramp_deals.dim_crm_opportunity_id IS NOT NULL 
                THEN ramp_deals."Overwrite_Term"
            WHEN mart_crm_opportunity.ssp_id IS NOT NULL 
                AND mart_crm_opportunity.opportunity_category LIKE '%Ramp Deal%'
                THEN mart_crm_opportunity.opportunity_term 
            ELSE mart_crm_opportunity.opportunity_term
        END AS opportunity_term_prep
    FROM mart_crm_opportunity
    LEFT JOIN ramp_deals
        ON mart_crm_opportunity.dim_crm_opportunity_id = ramp_deals.dim_crm_opportunity_id
        
), renewal_linking AS (

    SELECT *
    FROM dim_subscription
    WHERE zuora_renewal_subscription_name != '' -- where renewal is not empty, renewal is populated, this is last in lineage FALSE, already renewed
    
), is_last_in_lineage AS (

    SELECT DISTINCT
        subscription_name,
        'FALSE' AS is_last_in_lineage
    FROM renewal_linking

 -- PART 2 - the actual 'Day 5 ATR Detail (Subscription Detail)'   
    
), snapshot_dates AS (

    SELECT DISTINCT
        first_day_of_month,
        CASE 
            WHEN first_day_of_month < '2024-03-01'
                THEN snapshot_date_fpa
            ELSE snapshot_date_fpa_fifth
        END AS snapshot_date_fpa
    FROM dim_date
    
), fct_available_to_renew_snapshot AS (

    SELECT *
    FROM fct_available_to_renew_snapshot_model
    
), opportunity_term_group AS (

    SELECT
    dim_subscription.dim_subscription_id,
    dim_subscription.current_term,
    mart_crm_opportunity.dim_crm_opportunity_id,
      CASE
        WHEN mart_crm_opportunity.opportunity_term = 0
          THEN '0 Years'
        WHEN mart_crm_opportunity.opportunity_term <= 12
          THEN '1 Year'
        WHEN mart_crm_opportunity.opportunity_term > 12
          AND mart_crm_opportunity.opportunity_term <= 24
            THEN '2 Years'
        WHEN mart_crm_opportunity.opportunity_term > 24
          AND mart_crm_opportunity.opportunity_term <= 36
            THEN '3 Years'
        WHEN mart_crm_opportunity.opportunity_term > 36
          THEN '4 Years+'
        WHEN mart_crm_opportunity.opportunity_term IS NULL
          THEN 'No Opportunity Term'
      END                                                                                                               AS opportunity_term_group
      FROM dim_subscription
    LEFT JOIN mart_crm_opportunity
    ON dim_subscription.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id

    ), prep AS (
    
    SELECT
        fct_available_to_renew_snapshot.*,
        snapshot_dates.first_day_of_month,
        opportunity_term_group.current_term,
        opportunity_term_group.opportunity_term_group,
        dim_product_detail.product_tier_name,
        dim_product_detail.product_delivery_type
    FROM fct_available_to_renew_snapshot
    INNER JOIN snapshot_dates
        ON fct_available_to_renew_snapshot.snapshot_date = snapshot_dates.snapshot_date_fpa
    LEFT JOIN dim_product_detail
        ON fct_available_to_renew_snapshot.dim_product_detail_id = dim_product_detail.dim_product_detail_id
    LEFT JOIN opportunity_term_group
        ON opportunity_term_group.dim_subscription_id = fct_available_to_renew_snapshot.dim_subscription_id

), final AS (
    
    SELECT
        prep.snapshot_date,
        prep.fiscal_year AS renewal_fiscal_year,
        prep.renewal_month,
        prep.fiscal_quarter_name_fy,
        prep.subscription_name,
        prep.product_tier_name,
        prep.product_delivery_type,
        prep.opportunity_term_group,
        prep.dim_crm_opportunity_id,
        mart_crm_opportunity.subscription_type,
        mart_crm_opportunity.opportunity_category,
        prep.dim_crm_account_id, 
        prep.parent_crm_account_name,
        CASE 
            WHEN opportunity_term_prep.opportunity_term_prep IS NULL 
                THEN prep.current_term
            WHEN (mart_crm_opportunity.opportunity_category = 'Standard' AND IFNULL(mart_crm_opportunity.subscription_type, 'Renewal') IN ('New Business', 'Renewal'))
                OR mart_crm_opportunity.opportunity_category LIKE '%Ramp Deal%' 
                THEN opportunity_term_prep.opportunity_term_prep 
            ELSE prep.current_term 
        END AS adjusted_term,
        CASE 
            WHEN adjusted_term <= 12 THEN '1 Year'
            WHEN adjusted_term <= 24 THEN '2 Years'
            WHEN adjusted_term <= 36 THEN '3 Years'
            WHEN adjusted_term > 36 THEN '4 Years+'
        END AS adjusted_term_group,
        CASE
            WHEN is_last_in_lineage.is_last_in_lineage IS NULL THEN TRUE
            ELSE is_last_in_lineage.is_last_in_lineage
        END AS is_last_in_lineage,
        SUM(prep.arr) AS arr
    FROM prep
    LEFT JOIN mart_crm_opportunity
        ON prep.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
    LEFT JOIN opportunity_term_prep
        ON prep.dim_crm_opportunity_id = opportunity_term_prep.dim_crm_opportunity_id
    LEFT JOIN is_last_in_lineage
        ON is_last_in_lineage.subscription_name = prep.subscription_name
    {{ dbt_utils.group_by(16) }}
    ORDER BY 1 DESC, 2 DESC, 3, 4, 5

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2025-01-28",
updated_date="2025-01-28"
) }}