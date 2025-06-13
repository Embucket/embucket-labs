{{ config(
    tags=["six_hourly"],
    materialized="incremental",
    unique_key="crm_opportunity_snapshot_id",
    on_schema_change="sync_all_columns"
) }}

{{ simple_cte([
    ('net_iacv_to_net_arr_ratio', 'net_iacv_to_net_arr_ratio'),
    ('dim_date', 'dim_date'),
    ('sfdc_opportunity_stage', 'sfdc_opportunity_stage_source'),
    ('sfdc_record_type_source', 'sfdc_record_type_source'),
    ('sfdc_account_snapshots_source','sfdc_account_snapshots_source'),
    ('sfdc_account_snapshot','prep_crm_account_daily_snapshot'),
    ('sfdc_user_snapshot','prep_crm_user_daily_snapshot')
]) }},

sfdc_opportunity AS (

  SELECT *
  FROM {{ ref('prep_crm_opportunity_daily') }}
  {% if is_incremental() %}
    WHERE 
      -- First run in a new day: Get all data since the last successful run, 
      -- including the last successful run's data to update the is_live flag.
      (
        CURRENT_DATE() > (SELECT MAX(snapshot_date) FROM {{ this }})
        AND snapshot_date >= (SELECT MAX(snapshot_date) FROM {{ this }})
      )
      -- Subsequent runs on the same day: Only get new/updated records since the last run.
      OR dbt_valid_from > (SELECT MAX(dbt_valid_from) FROM {{ this }})
  {% endif %}

),

base AS (

  SELECT
    sfdc_opportunity.*,
    sfdc_account_snapshot.dim_crm_user_id        AS dim_crm_account_user_id,
    sfdc_account_snapshot.parent_crm_account_geo,
    sfdc_account_snapshot.crm_account_owner_sales_segment,
    sfdc_account_snapshot.crm_account_owner_geo,
    sfdc_account_snapshot.crm_account_owner_region,
    sfdc_account_snapshot.crm_account_owner_area,
    sfdc_account_snapshot.crm_account_owner_sales_segment_geo_region_area,
    account_owner.dim_crm_user_hierarchy_sk      AS dim_crm_user_hierarchy_account_user_sk,
    account_owner.user_role_name                 AS crm_account_owner_role,
    account_owner.user_role_level_1              AS crm_account_owner_role_level_1,
    account_owner.user_role_level_2              AS crm_account_owner_role_level_2,
    account_owner.user_role_level_3              AS crm_account_owner_role_level_3,
    account_owner.user_role_level_4              AS crm_account_owner_role_level_4,
    account_owner.user_role_level_5              AS crm_account_owner_role_level_5,
    account_owner.title                          AS crm_account_owner_title,
    fulfillment_partner.crm_account_name         AS fulfillment_partner_account_name,
    fulfillment_partner.partner_track            AS fulfillment_partner_partner_track,
    partner_account.crm_account_name             AS partner_account_account_name,
    partner_account.partner_track                AS partner_account_partner_track,
    sfdc_account_snapshot.is_jihu_account,
    sfdc_account_snapshot.dim_parent_crm_account_id,
    CASE
      WHEN sfdc_opportunity.user_segment_stamped IS NULL
        OR is_open = 1
        THEN sfdc_account_snapshot.crm_account_owner_sales_segment
      ELSE sfdc_opportunity.user_segment_stamped
    END                                          AS opportunity_owner_user_segment,
    sfdc_user_snapshot.user_role_name            AS opportunity_owner_role,
    sfdc_user_snapshot.user_role_level_1         AS crm_opp_owner_role_level_1,
    sfdc_user_snapshot.user_role_level_2         AS crm_opp_owner_role_level_2,
    sfdc_user_snapshot.user_role_level_3         AS crm_opp_owner_role_level_3,
    sfdc_user_snapshot.user_role_level_4         AS crm_opp_owner_role_level_4,
    sfdc_user_snapshot.user_role_level_5         AS crm_opp_owner_role_level_5,
    sfdc_user_snapshot.title                     AS opportunity_owner_title,
    sfdc_account_snapshot.crm_account_owner_role AS opportunity_account_owner_role
  FROM sfdc_opportunity
  LEFT JOIN sfdc_account_snapshot
    ON sfdc_opportunity.dim_crm_account_id = sfdc_account_snapshot.dim_crm_account_id
      AND sfdc_opportunity.snapshot_id = sfdc_account_snapshot.snapshot_id
  LEFT JOIN sfdc_account_snapshot AS fulfillment_partner
    ON fulfillment_partner = fulfillment_partner.dim_crm_account_id
      AND sfdc_opportunity.snapshot_id = fulfillment_partner.snapshot_id
  LEFT JOIN sfdc_account_snapshot AS partner_account
    ON sfdc_opportunity.partner_account = partner_account.dim_crm_account_id
      AND sfdc_opportunity.snapshot_id = partner_account.snapshot_id
  LEFT JOIN sfdc_user_snapshot
    ON sfdc_opportunity.dim_crm_user_id = sfdc_user_snapshot.dim_crm_user_id
      AND sfdc_opportunity.snapshot_id = sfdc_user_snapshot.snapshot_id
  LEFT JOIN sfdc_user_snapshot AS account_owner
    ON sfdc_account_snapshot.dim_crm_user_id = account_owner.dim_crm_user_id
      AND sfdc_opportunity.snapshot_id = account_owner.snapshot_id

),

first_contact AS (

  SELECT
    opportunity_id,                                                             -- opportunity_id
    contact_id                 AS sfdc_contact_id,
    {{ dbt_utils.generate_surrogate_key(['contact_id']) }} AS dim_crm_person_id
  FROM {{ ref('sfdc_opportunity_contact_role_source') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY created_date ASC) = 1

),

account_history_final AS (

  SELECT
    account_id_18                     AS dim_crm_account_id,
    owner_id                          AS dim_crm_user_id,
    ultimate_parent_id                AS dim_crm_parent_account_id,
    abm_tier_1_date,
    abm_tier_2_date,
    abm_tier,
    CAST(MIN(dbt_valid_from) AS DATE) AS valid_from,
    CAST(MAX(dbt_valid_to) AS DATE)   AS valid_to
  FROM sfdc_account_snapshots_source
  WHERE abm_tier_1_date >= '2022-02-01'
    OR abm_tier_2_date >= '2022-02-01'
  {{ dbt_utils.group_by(n=6) }}

),

attribution_touchpoints AS (

  SELECT *
  FROM {{ ref('sfdc_bizible_attribution_touchpoint_source') }}
  WHERE is_deleted = 'FALSE'

),

opportunity_attribution_metrics AS (

  --the number of attribution touches a given opp has in total
  --linear attribution IACV of an opp / all touches (count_touches) for each opp - weighted by the number of touches in the given bucket (campaign,channel,etc)
  SELECT
    opportunity_id                                        AS dim_crm_opportunity_id,
    COUNT(DISTINCT attribution_touchpoints.touchpoint_id) AS count_crm_attribution_touchpoints,
    COUNT(DISTINCT attribution_touchpoints.campaign_id)   AS count_campaigns
  FROM attribution_touchpoints
  GROUP BY 1

),

sfdc_zqu_quote_source AS (

  SELECT *
  FROM {{ ref('sfdc_zqu_quote_source') }}
  WHERE is_deleted = FALSE

),

quote AS (

  SELECT DISTINCT
    sfdc_zqu_quote_source.zqu__opportunity              AS dim_crm_opportunity_id,
    sfdc_zqu_quote_source.zqu_quote_id                  AS dim_quote_id,
    CAST(sfdc_zqu_quote_source.zqu__start_date AS DATE) AS quote_start_date
  FROM sfdc_zqu_quote_source
  INNER JOIN base
    ON sfdc_zqu_quote_source.zqu__opportunity = base.dim_crm_opportunity_id
  WHERE base.stage_name IN ('Closed Won', '8-Closed Lost')
    AND sfdc_zqu_quote_source.zqu__primary = TRUE
    AND NOT sfdc_zqu_quote_source.is_deleted
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY sfdc_zqu_quote_source.zqu__opportunity ORDER BY sfdc_zqu_quote_source.created_date DESC)) = 1

),

sao_base AS (

  SELECT
    --IDs
    base.dim_crm_opportunity_id,

    --Opp Data  

    base.sales_accepted_date,
    IFF(base.is_edu_oss = 0 AND base.stage_name != '10-Duplicate' AND sales_accepted_date BETWEEN valid_from AND valid_to, TRUE, FALSE) AS is_abm_tier_sao
  FROM base
  LEFT JOIN account_history_final
    ON base.dim_crm_account_id = account_history_final.dim_crm_account_id
  WHERE abm_tier IS NOT NULL
    AND sales_accepted_date IS NOT NULL
    AND sales_accepted_date >= '2022-02-01'
    AND (
      abm_tier_1_date IS NOT NULL
      OR abm_tier_2_date IS NOT NULL
    )
    AND is_abm_tier_sao = TRUE

),

cw_base AS (

  SELECT
    --IDs
    base.dim_crm_opportunity_id,

    --Opp Data  
    base.close_date,
    IFF(stage_name = 'Closed Won' AND close_date BETWEEN valid_from AND valid_to, TRUE, FALSE) AS is_abm_tier_closed_won
  FROM base
  LEFT JOIN account_history_final
    ON base.dim_crm_account_id = account_history_final.dim_crm_account_id
  WHERE abm_tier IS NOT NULL
    AND close_date IS NOT NULL
    AND close_date >= '2022-02-01'
    AND (
      abm_tier_1_date IS NOT NULL
      OR abm_tier_2_date IS NOT NULL
    )
    AND is_abm_tier_closed_won = TRUE

),

abm_tier_id AS (

  SELECT dim_crm_opportunity_id
  FROM sao_base

  UNION

  SELECT dim_crm_opportunity_id
  FROM cw_base

),

abm_tier_unioned AS (

  SELECT DISTINCT
    abm_tier_id.dim_crm_opportunity_id,
    is_abm_tier_sao,
    is_abm_tier_closed_won
  FROM abm_tier_id
  LEFT JOIN sao_base
    ON abm_tier_id.dim_crm_opportunity_id = sao_base.dim_crm_opportunity_id
  LEFT JOIN cw_base
    ON abm_tier_id.dim_crm_opportunity_id = cw_base.dim_crm_opportunity_id

),

final AS (

  SELECT
    base.*,

    -- date fields
    close_date.first_day_of_fiscal_quarter                                                            AS close_fiscal_quarter_date,
    close_date.fiscal_quarter_name_fy                                                                 AS close_fiscal_quarter_name,
    90 - DATEDIFF(DAY, base.snapshot_date, close_date.last_day_of_fiscal_quarter)                     AS close_day_of_fiscal_quarter_normalised,
    arr_created_date.fiscal_quarter_name_fy                                                           AS arr_created_fiscal_quarter_name,
    arr_created_date.first_day_of_fiscal_quarter                                                      AS arr_created_fiscal_quarter_date,
    created_date.fiscal_quarter_name_fy                                                               AS created_fiscal_quarter_name,
    created_date.first_day_of_fiscal_quarter                                                          AS created_fiscal_quarter_date,
    subscription_start_date.fiscal_quarter_name_fy                                                    AS subscription_start_date_fiscal_quarter_name,
    subscription_start_date.first_day_of_fiscal_quarter                                               AS subscription_start_date_fiscal_quarter_date,
    COALESCE(net_iacv_to_net_arr_ratio.ratio_net_iacv_to_net_arr, 0)                                  AS segment_order_type_iacv_to_net_arr_ratio,

    -- net arr
    CASE
      WHEN sfdc_opportunity_stage.is_won = 1 -- only consider won deals
        AND base.opportunity_category_live != 'Contract Reset' -- contract resets have a special way of calculating net iacv
        AND COALESCE(base.raw_net_arr, 0) != 0
        AND COALESCE(base.net_incremental_acv, 0) != 0
        THEN COALESCE(base.raw_net_arr / base.net_incremental_acv, 0)
    END                                                                                               AS opportunity_based_iacv_to_net_arr_ratio,
    CASE
      WHEN base.stage_name NOT IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')  -- OPEN DEAL
        THEN COALESCE(base.incremental_acv, 0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio, 0)
      WHEN base.stage_name IN ('8-Closed Lost')                       -- CLOSED LOST DEAL and no Net IACV
        AND COALESCE(base.net_incremental_acv, 0) = 0
        THEN COALESCE(base.incremental_acv, 0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio, 0)
      WHEN base.stage_name IN ('8-Closed Lost', 'Closed Won')         -- REST of CLOSED DEAL
        THEN COALESCE(base.net_incremental_acv, 0) * COALESCE(opportunity_based_iacv_to_net_arr_ratio, segment_order_type_iacv_to_net_arr_ratio)
    END                                                                                               AS calculated_from_ratio_net_arr,
    -- For opportunities before start of FY22, as Net ARR was WIP, there are a lot of opties with IACV or Net IACV and no Net ARR
    -- Those were later fixed in the opportunity object but stayed in the snapshot table.
    -- To account for those issues and give a directionally correct answer, we apply a ratio to everything before FY22
    CASE
      WHEN CAST(base.snapshot_date AS DATE) < '2021-02-01' -- All deals before cutoff and that were not updated to Net ARR
        THEN calculated_from_ratio_net_arr
      WHEN CAST(base.snapshot_date AS DATE) >= '2021-02-01'  -- After cutoff date, for all deals earlier than FY19 that are closed and have no net arr
        AND CAST(base.close_date AS DATE) < '2018-02-01'
        AND base.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')
        AND COALESCE(base.raw_net_arr, 0) = 0
        THEN calculated_from_ratio_net_arr
      ELSE COALESCE(base.raw_net_arr, 0) -- Rest of deals after cut off date
    END                                                                                               AS net_arr,

    -- opportunity flags
    abm_tier_unioned.is_abm_tier_sao,
    abm_tier_unioned.is_abm_tier_closed_won,
    COALESCE(
      base.opportunity_term_base, 
      DATEDIFF('month', quote.quote_start_date, base.subscription_end_date)
      )                                                                                                AS opportunity_term,

    -- flags
    sfdc_opportunity_stage.is_active,
    sfdc_opportunity_stage.is_won,
    IFF(base.opportunity_category_live IN ('Decommission'), 1, 0)                                     AS is_decommissed,
    COALESCE(
      base.sales_accepted_date IS NOT NULL
      AND base.is_edu_oss_live = 0
      AND base.stage_name != '10-Duplicate', FALSE
    )                                                                                                 AS is_sao,
    COALESCE(
      is_sao = TRUE
      AND base.sales_qualified_source_live IN (
        'SDR Generated',
        'BDR Generated'
      ), FALSE
    )                                                                                                 AS is_sdr_sao,
    COALESCE((
      (base.subscription_type = 'Renewal' AND base.stage_name = '8-Closed Lost')
      OR base.stage_name = 'Closed Won'
    )
    AND (base.is_jihu_account_live != TRUE OR base.is_jihu_account_live IS NULL), FALSE)              AS is_net_arr_closed_deal,
    COALESCE((
      base.new_logo_count = 1
      OR base.new_logo_count = -1
    )
    AND (base.is_jihu_account_live != TRUE OR base.is_jihu_account_live IS NULL), FALSE)              AS is_new_logo_first_order,
    -- align is_booked_net_arr with fpa_master_bookings_flag definition from salesforce: https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/1805
    -- coalesce both flags so we don't have NULL values for records before the fpa_master_bookings_flag was created
    COALESCE(
      base.fpa_master_bookings_flag,
      CASE
        WHEN (base.is_jihu_account_live != TRUE OR base.is_jihu_account_live IS NULL)
          AND (
            sfdc_opportunity_stage.is_won = 1
            OR (
              base.is_renewal = 1
              AND is_lost = 1
            )
          )
          THEN 1
        ELSE 0
      END
    )                                                                                                 AS is_booked_net_arr,
    /*
        Stop coalescing is_pipeline_created_eligible and is_net_arr_pipeline_created
      Definition changed for is_pipeline_created_eligible and if we coalesce both, the values will be inaccurate for
      snapshots before the definition changed in SFDC: https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/5331
      Use is_net_arr_pipeline_created as the SSOT
      */
    CASE
      WHEN base.order_type_live IN ('1. New - First Order', '2. New - Connected', '3. Growth')
        AND base.is_edu_oss_live = 0
        AND arr_created_date.first_day_of_fiscal_quarter IS NOT NULL
        AND base.opportunity_category_live IN ('Standard', 'Internal Correction', 'Ramp Deal', 'Credit', 'Contract Reset', 'Contract Reset/Ramp Deal')
        AND base.stage_name NOT IN ('00-Pre Opportunity', '10-Duplicate', '9-Unqualified', '0-Pending Acceptance')
        AND (
          net_arr > 0
          OR base.opportunity_category_live = 'Credit'
        )
        AND base.sales_qualified_source_live != 'Web Direct Generated'
        AND (base.is_jihu_account_live != TRUE OR base.is_jihu_account_live IS NULL)
        AND base.stage_1_discovery_date IS NOT NULL
        THEN 1
      ELSE 0
    END                                                                                               AS is_net_arr_pipeline_created,
    COALESCE(
      base.close_date <= CURRENT_DATE()
      AND base.is_closed = 'TRUE'
      AND base.is_edu_oss_live = 0
      AND (base.is_jihu_account_live != TRUE OR base.is_jihu_account_live IS NULL)
      AND (base.reason_for_loss IS NULL OR base.reason_for_loss != 'Merged into another opportunity')
      AND base.sales_qualified_source_live != 'Web Direct Generated'
      AND base.parent_crm_account_geo_live != 'JIHU'
      AND base.stage_name NOT IN ('10-Duplicate', '9-Unqualified'), FALSE
    )                                                                                                 AS is_win_rate_calc,
    COALESCE(
      sfdc_opportunity_stage.is_won = 'TRUE'
      AND base.is_closed = 'TRUE'
      AND base.is_edu_oss_live = 0, FALSE
    )                                                                                                 AS is_closed_won,
    CASE
      WHEN LOWER(base.order_type_grouped_live) LIKE ANY ('%growth%', '%new%')
        AND base.is_edu_oss_live = 0
        AND base.is_stage_1_plus = 1
        AND base.forecast_category_name != 'Omitted'
        AND base.is_open = 1
        AND (base.is_jihu_account_live != TRUE OR base.is_jihu_account_live IS NULL)
        THEN 1
      ELSE 0
    END                                                                                               AS is_eligible_open_pipeline,
    CASE
      WHEN base.sales_accepted_date IS NOT NULL
        AND base.is_edu_oss_live = 0
        AND base.is_deleted = 0
        THEN 1
      ELSE 0
    END                                                                                               AS is_eligible_sao,
    CASE
      WHEN base.is_edu_oss_live = 0
        AND base.is_deleted = 0
        -- For                                                          ASP we care mainly about add on, new business, excluding contraction / churn
        AND base.order_type_live IN ('1. New - First Order', '2. New - Connected', '3. Growth')
        -- Exclude Decomissioned as they are not aligned to the real owner
        -- Contract Reset, Decomission
        AND base.opportunity_category_live IN ('Standard', 'Ramp Deal', 'Internal Correction')
        -- Exclude Deals with nARR < 0
        AND net_arr > 0
        THEN 1
      ELSE 0
    END                                                                                               AS is_eligible_asp_analysis,
    CASE
      WHEN base.close_date <= CURRENT_DATE()
        AND is_booked_net_arr = TRUE
        AND base.is_edu_oss_live = 0
        AND (base.is_jihu_account_live != TRUE OR base.is_jihu_account_live IS NULL)
        AND (base.reason_for_loss IS NULL OR base.reason_for_loss != 'Merged into another opportunity')
        AND base.sales_qualified_source_live != 'Web Direct Generated'
        AND base.deal_path_live != 'Web Direct'
        AND base.order_type_live IN ('1. New - First Order', '2. New - Connected', '3. Growth', '4. Contraction', '6. Churn - Final', '5. Churn - Partial')
        AND base.parent_crm_account_geo_live != 'JIHU'
        AND (base.opportunity_category_live IN ('Standard') OR (
          /* Include only first year ramp deals. The ssp_id should be either equal to the SFDC id (18)
            or equal to the SFDC id (15) for first year ramp deals */
          base.opportunity_category_live = 'Ramp Deal'
          AND LEFT(base.dim_crm_opportunity_id, LENGTH(base.ssp_id)) = base.ssp_id
        ))
        THEN 1
      ELSE 0
    END                                                                                               AS is_eligible_age_analysis,
    CASE
      WHEN base.is_edu_oss_live = 0
        AND base.is_deleted = 0
        AND (
          sfdc_opportunity_stage.is_won = 1
          OR (base.is_renewal = 1 AND is_lost = 1)
        )
        AND base.order_type_live IN ('1. New - First Order', '2. New - Connected', '3. Growth', '4. Contraction', '6. Churn - Final', '5. Churn - Partial')
        THEN 1
      ELSE 0
    END                                                                                               AS is_eligible_net_arr,
    CASE
      WHEN base.is_edu_oss_live = 0
        AND base.is_deleted = 0
        AND base.order_type_live IN ('4. Contraction', '6. Churn - Final', '5. Churn - Partial')
        THEN 1
      ELSE 0
    END                                                                                               AS is_eligible_churn_contraction,
    IFF(base.opportunity_category_live IN ('Credit'), 1, 0)                                           AS is_credit,
    IFF(base.opportunity_category_live IN ('Contract Reset'), 1, 0)                                   AS is_contract_reset,

    -- alliance type fields
    {{ alliance_partner_current('base.fulfillment_partner_account_name', 'base.partner_account_account_name', 'base.partner_track', 'base.resale_partner_track', 'base.deal_path', 'base.is_focus_partner') }}
                                                                                                      AS alliance_type_current,
    {{ alliance_partner_short_current('base.fulfillment_partner_account_name', 'base.partner_account_account_name', 'base.partner_track', 'base.resale_partner_track', 'base.deal_path', 'base.is_focus_partner') }}
                                                                                                      AS alliance_type_short_current,
    {{ alliance_partner('base.fulfillment_partner_account_name', 'base.partner_account_account_name', 'base.close_date', 'base.partner_track', 'base.resale_partner_track', 'base.deal_path', 'base.is_focus_partner') }}
                                                                                                      AS alliance_type,
    {{ alliance_partner_short('base.fulfillment_partner_account_name', 'base.partner_account_account_name', 'base.close_date', 'base.partner_track', 'base.resale_partner_track', 'base.deal_path', 'base.is_focus_partner') }}
                                                                                                      AS alliance_type_short,
    fulfillment_partner_account_name                                                                  AS resale_partner_name,

    --  quote information
    quote.dim_quote_id,
    quote.quote_start_date,
    -- contact information
    first_contact.dim_crm_person_id,
    first_contact.sfdc_contact_id,
    -- Record type information
    sfdc_record_type_source.record_type_name,
    -- attribution information
    opportunity_attribution_metrics.count_crm_attribution_touchpoints,
    opportunity_attribution_metrics.count_campaigns,
    base.incremental_acv / opportunity_attribution_metrics.count_crm_attribution_touchpoints          AS weighted_linear_iacv,

    -- opportunity attributes
    CASE
      WHEN net_arr > 0 AND net_arr < 5000
        THEN '1 - Small (<5k)'
      WHEN net_arr >= 5000 AND net_arr < 25000
        THEN '2 - Medium (5k - 25k)'
      WHEN net_arr >= 25000 AND net_arr < 100000
        THEN '3 - Big (25k - 100k)'
      WHEN net_arr >= 100000
        THEN '4 - Jumbo (>100k)'
      ELSE 'Other'
    END                                                                                               AS deal_size,
    CASE
      WHEN net_arr > 0 AND net_arr < 1000
        THEN '1. (0k -1k)'
      WHEN net_arr >= 1000 AND net_arr < 10000
        THEN '2. (1k - 10k)'
      WHEN net_arr >= 10000 AND net_arr < 50000
        THEN '3. (10k - 50k)'
      WHEN net_arr >= 50000 AND net_arr < 100000
        THEN '4. (50k - 100k)'
      WHEN net_arr >= 100000 AND net_arr < 250000
        THEN '5. (100k - 250k)'
      WHEN net_arr >= 250000 AND net_arr < 500000
        THEN '6. (250k - 500k)'
      WHEN net_arr >= 500000 AND net_arr < 1000000
        THEN '7. (500k-1000k)'
      WHEN net_arr >= 1000000
        THEN '8. (>1000k)'
      ELSE 'Other'
    END                                                                                               AS calculated_deal_size,
    CASE
      WHEN net_arr > -5000
        THEN '1. < 5k'
      WHEN net_arr > -20000 AND net_arr <= -5000
        THEN '2. 5k-20k'
      WHEN net_arr > -50000 AND net_arr <= -20000
        THEN '3. 20k-50k'
      WHEN net_arr > -100000 AND net_arr <= -50000
        THEN '4. 50k-100k'
      WHEN net_arr < -100000
        THEN '5. 100k+'
    END                                                                                               AS churned_contraction_net_arr_bucket,
    CASE
      WHEN net_arr > -5000
        AND is_eligible_churn_contraction = 1
        THEN '1. < 5k'
      WHEN net_arr > -20000
        AND net_arr <= -5000
        AND is_eligible_churn_contraction = 1
        THEN '2. 5k-20k'
      WHEN net_arr > -50000
        AND net_arr <= -20000
        AND is_eligible_churn_contraction = 1
        THEN '3. 20k-50k'
      WHEN net_arr > -100000
        AND net_arr <= -50000
        AND is_eligible_churn_contraction = 1
        THEN '4. 50k-100k'
      WHEN net_arr < -100000
        AND is_eligible_churn_contraction = 1
        THEN '5. 100k+'
    END                                                                                               AS churn_contraction_net_arr_bucket,
    CASE
      WHEN sfdc_opportunity_stage.is_won = 1
        THEN '1.Won'
      WHEN is_lost = 1
        THEN '2.Lost'
      WHEN base.is_open = 1
        THEN '0. Open'
      ELSE 'N/A'
    END                                                                                               AS stage_category,
    CASE
      WHEN base.order_type_live = '1. New - First Order'
        THEN '1. New'
      WHEN base.order_type_live IN ('2. New - Connected', '3. Growth', '5. Churn - Partial', '6. Churn - Final', '4. Contraction')
        THEN '2. Growth'
      ELSE '3. Other'
    END                                                                                               AS deal_group,
    CASE
      WHEN base.order_type_live = '1. New - First Order'
        THEN '1. New'
      WHEN base.order_type_live IN ('2. New - Connected', '3. Growth')
        THEN '2. Growth'
      WHEN base.order_type_live IN ('4. Contraction')
        THEN '3. Contraction'
      WHEN base.order_type_live IN ('5. Churn - Partial', '6. Churn - Final')
        THEN '4. Churn'
      ELSE '5. Other'
    END                                                                                               AS deal_category,
    CASE
      WHEN (
        (
          base.is_renewal = 1
          AND is_lost = 1
        )
        OR sfdc_opportunity_stage.is_won = 1
      )
      AND base.order_type_live IN ('4. Contraction', '5. Churn - Partial')
        THEN 'Contraction'
      WHEN (
        (
          base.is_renewal = 1
          AND is_lost = 1
        )
        OR sfdc_opportunity_stage.is_won = 1
      )
      AND base.order_type_live = '6. Churn - Final'
        THEN 'Churn'
    END                                                                                               AS churn_contraction_type,
    CASE
      WHEN base.is_renewal = 1
        AND subscription_start_date_fiscal_quarter_date >= close_fiscal_quarter_date
        THEN 'On-Time'
      WHEN base.is_renewal = 1
        AND subscription_start_date_fiscal_quarter_date < close_fiscal_quarter_date
        THEN 'Late'
    END                                                                                               AS renewal_timing_status,
    CASE
      WHEN base.deal_path_live = 'Direct'
        THEN 'Direct'
      WHEN base.deal_path_live = 'Web Direct'
        THEN 'Web Direct'
      WHEN base.deal_path_live = 'Partner'
        AND base.sales_qualified_source_live = 'Partner Generated'
        THEN 'Partner Sourced'
      WHEN base.deal_path_live = 'Partner'
        AND base.sales_qualified_source_live != 'Partner Generated'
        THEN 'Partner Co-Sell'
    END                                                                                               AS deal_path_engagement,

    -- counts and arr totals by pipeline stage
    CASE
      WHEN is_decommissed = 1
        THEN -1
      WHEN is_credit = 1
        THEN 0
      ELSE 1
    END                                                                                               AS calculated_deal_count,
    IFF(is_eligible_open_pipeline = 1 AND base.is_stage_1_plus = 1, calculated_deal_count, 0)         AS open_1plus_deal_count,
    IFF(is_eligible_open_pipeline = 1 AND base.is_stage_3_plus = 1, calculated_deal_count, 0)         AS open_3plus_deal_count,
    IFF(is_eligible_open_pipeline = 1 AND base.is_stage_4_plus = 1, calculated_deal_count, 0)         AS open_4plus_deal_count,
    IFF(is_booked_net_arr = 1, calculated_deal_count, 0)                                              AS booked_deal_count,
    IFF(is_eligible_churn_contraction = 1, calculated_deal_count, 0)                                  AS churned_contraction_deal_count,
    CASE 
      WHEN (
            (base.is_renewal = 1 AND is_lost = 1)
            OR sfdc_opportunity_stage.is_won = 1
          )
          AND is_eligible_churn_contraction = 1 
      THEN calculated_deal_count
      ELSE 0 
    END                                                                                               AS booked_churned_contraction_deal_count,
    CASE 
      WHEN (
            (base.is_renewal = 1 AND is_lost = 1)
            OR sfdc_opportunity_stage.is_won = 1
          )
          AND is_eligible_churn_contraction = 1 
      THEN net_arr 
      ELSE 0 
    END                                                                                               AS booked_churned_contraction_net_arr,
    IFF(is_eligible_churn_contraction = 1, net_arr, 0)                                                AS churned_contraction_net_arr,
    IFF(is_eligible_open_pipeline = 1, net_arr, 0)                                                    AS open_1plus_net_arr,
    IFF(is_eligible_open_pipeline = 1 AND base.is_stage_3_plus = 1, net_arr, 0)                       AS open_3plus_net_arr,
    IFF(is_eligible_open_pipeline = 1 AND base.is_stage_4_plus = 1, net_arr, 0)                       AS open_4plus_net_arr,
    IFF(is_booked_net_arr = 1, net_arr, 0)                                                            AS booked_net_arr,
    CASE
      WHEN base.deal_path_live = 'Partner'
        THEN REPLACE(COALESCE(partner_track, partner_account_partner_track, fulfillment_partner_partner_track, 'Open'), 'select', 'Select')
      ELSE 'Direct'
    END                                                                                               AS calculated_partner_track,
    CASE
      WHEN
        base.dim_parent_crm_account_id IN (
          '001610000111bA3',
          '0016100001F4xla',
          '0016100001CXGCs',
          '00161000015O9Yn',
          '0016100001b9Jsc'
        )
        AND base.close_date < '2020-08-01'
        THEN 1
      -- NF 2021 - Pubsec extreme deals
      WHEN
        base.dim_crm_opportunity_id IN ('0064M00000WtZKUQA3', '0064M00000Xb975QAB')
        AND (base.snapshot_date < '2021-05-01')
        THEN 1
      -- exclude vision opps from FY21-Q2
      WHEN arr_created_fiscal_quarter_name = 'FY21-Q2'
        AND base.snapshot_day_of_fiscal_quarter_normalised = 90
        AND base.stage_name IN (
          '00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying'
        )
        THEN 1
      -- NF 20220415 PubSec duplicated deals on Pipe Gen -- Lockheed Martin GV - 40000 Ultimate Renewal
      WHEN
        base.dim_crm_opportunity_id IN (
          '0064M00000ZGpfQQAT', '0064M00000ZGpfVQAT', '0064M00000ZGpfGQAT'
        )
        THEN 1
      -- remove test accounts
      WHEN
        base.dim_crm_account_id = '0014M00001kGcORQA0'
        THEN 1
      --remove test accounts
      WHEN (
        base.dim_parent_crm_account_id = ('0016100001YUkWVAA1')
        OR base.dim_crm_account_id IS NULL
      )
        THEN 1
      -- remove jihu accounts
      WHEN base.is_jihu_account_live = 1
        THEN 1
      -- remove deleted opps
      WHEN base.is_deleted = 1
        THEN 1
      ELSE 0
    END                                                                                               AS is_excluded_from_pipeline_created,
    CASE
      WHEN base.is_open = 1
        THEN DATEDIFF(DAYS, base.created_date, base.snapshot_date)
      WHEN base.is_open = 0 AND base.snapshot_date < base.close_date
        THEN DATEDIFF(DAYS, base.created_date, base.snapshot_date)
      ELSE DATEDIFF(DAYS, base.created_date, base.close_date)
    END                                                                                               AS calculated_age_in_days,
    CASE 
      WHEN arr_created_fiscal_quarter_date = close_fiscal_quarter_date 
          AND is_net_arr_pipeline_created = 1
      THEN net_arr
      ELSE 0 
    END                                                                                                  AS created_and_won_same_quarter_net_arr,
    CASE 
      WHEN arr_created_date.fiscal_quarter_name_fy = base.snapshot_fiscal_quarter_name 
          AND is_net_arr_pipeline_created = 1
      THEN net_arr
      ELSE 0 
    END                                                                                                  AS created_in_snapshot_quarter_net_arr,
    CASE 
      WHEN arr_created_date.fiscal_quarter_name_fy = base.snapshot_fiscal_quarter_name 
          AND is_net_arr_pipeline_created = 1
      THEN calculated_deal_count
      ELSE 0 
    END                                                                                                  AS created_in_snapshot_quarter_deal_count,
    CASE
      WHEN close_fiscal_year_live < 2024
        THEN CONCAT(
            UPPER(base.crm_opp_owner_sales_segment_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_geo_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_region_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_area_stamped_live),
            '-',
            close_fiscal_year
          )
      WHEN close_fiscal_year_live = 2024 AND LOWER(base.crm_opp_owner_business_unit_stamped_live) = 'comm'
        THEN CONCAT(
            UPPER(base.crm_opp_owner_business_unit_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_geo_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_sales_segment_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_region_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_area_stamped_live),
            '-',
            close_fiscal_year_live
          )
      WHEN close_fiscal_year_live = 2024 AND LOWER(base.crm_opp_owner_business_unit_stamped_live) = 'entg'
        THEN CONCAT(
            UPPER(base.crm_opp_owner_business_unit_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_geo_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_region_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_area_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_sales_segment_stamped_live),
            '-',
            close_fiscal_year_live
          )
      WHEN close_fiscal_year_live = 2024
        AND (base.crm_opp_owner_business_unit_stamped_live IS NOT NULL AND LOWER(base.crm_opp_owner_business_unit_stamped_live) NOT IN ('comm', 'entg')) -- some opps are closed by non-sales reps, so fill in their values completely
        THEN CONCAT(
            UPPER(base.crm_opp_owner_business_unit_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_sales_segment_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_geo_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_region_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_area_stamped_live),
            '-',
            close_fiscal_year_live
          )
      WHEN close_fiscal_year_live = 2024 AND base.crm_opp_owner_business_unit_stamped_live IS NULL -- done for data quality issues
        THEN CONCAT(
            UPPER(base.crm_opp_owner_sales_segment_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_geo_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_region_stamped_live),
            '-',
            UPPER(base.crm_opp_owner_area_stamped_live),
            '-',
            close_fiscal_year_live
          )
      WHEN close_fiscal_year_live >= 2025
        THEN CONCAT(

            UPPER(COALESCE(base.opportunity_owner_role_live, base.opportunity_account_owner_role_live)),
            '-',
            close_fiscal_year_live
          )
    END                                                                                               AS dim_crm_opp_owner_stamped_hierarchy_sk,
    UPPER(
      CASE 
          WHEN base.close_date_live < close_date_live.current_first_day_of_fiscal_year 
          THEN base.dim_crm_user_hierarchy_account_user_sk_live 
          ELSE dim_crm_opp_owner_stamped_hierarchy_sk 
      END
    )                                                                                                 AS dim_crm_current_account_set_hierarchy_sk,
    DATEDIFF(MONTH, arr_created_fiscal_quarter_date, close_fiscal_quarter_date)                       AS arr_created_to_close_diff,
    CASE
      WHEN arr_created_to_close_diff BETWEEN 0 AND 2 THEN 'CQ'
      WHEN arr_created_to_close_diff BETWEEN 3 AND 5 THEN 'CQ+1'
      WHEN arr_created_to_close_diff BETWEEN 6 AND 8 THEN 'CQ+2'
      WHEN arr_created_to_close_diff BETWEEN 9 AND 11 THEN 'CQ+3'
      WHEN arr_created_to_close_diff >= 12 THEN 'CQ+4 >='
    END                                                                                               AS landing_quarter_relative_to_arr_created_date,
    DATEDIFF(MONTH, base.snapshot_fiscal_quarter_date, close_fiscal_quarter_date)                     AS snapshot_to_close_diff,
    CASE
      WHEN snapshot_to_close_diff BETWEEN 0 AND 2 THEN 'CQ'
      WHEN snapshot_to_close_diff BETWEEN 3 AND 5 THEN 'CQ+1'
      WHEN snapshot_to_close_diff BETWEEN 6 AND 8 THEN 'CQ+2'
      WHEN snapshot_to_close_diff BETWEEN 9 AND 11 THEN 'CQ+3'
      WHEN snapshot_to_close_diff >= 12 THEN 'CQ+4 >='
    END                                                                                               AS landing_quarter_relative_to_snapshot_date,
    CASE
      WHEN base.is_renewal = 1
        AND is_eligible_age_analysis = 1
        THEN DATEDIFF(DAY, arr_created_date, close_date.date_actual)
      WHEN base.is_renewal = 0
        AND is_eligible_age_analysis = 1
        THEN DATEDIFF(DAY, base.created_date, close_date.date_actual)
    END                                                                                               AS cycle_time_in_days,
  -- Snapshot Quarter Metrics
  -- This code calculates sales metrics for each snapshot quarter
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = arr_created_fiscal_quarter_date 
          AND is_net_arr_pipeline_created = 1
      THEN net_arr
      ELSE NULL 
    END                                                                                                  AS created_arr_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
        AND is_closed_won = TRUE 
        AND is_win_rate_calc = TRUE
      THEN calculated_deal_count
      ELSE NULL 
    END                                                                                                  AS closed_won_opps_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
          AND is_win_rate_calc = TRUE
      THEN calculated_deal_count
      ELSE NULL 
    END                                                                                                  AS closed_opps_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
          AND is_booked_net_arr = TRUE
      THEN net_arr
      ELSE NULL 
    END                                                                                                  AS booked_net_arr_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = arr_created_fiscal_quarter_date 
          AND is_net_arr_pipeline_created = 1
      THEN calculated_deal_count
      ELSE NULL 
    END                                                                                                  AS created_deals_in_snapshot_quarter,
    CASE
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date
        AND base.is_renewal = 1
        AND is_eligible_age_analysis = 1
        THEN DATEDIFF(DAY, arr_created_date, close_date.date_actual)
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date
        AND base.is_renewal = 0
        AND is_eligible_age_analysis = 1
        THEN DATEDIFF(DAY, base.created_date, close_date.date_actual)
    END                                                                                                  AS cycle_time_in_days_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
          AND is_booked_net_arr = TRUE 
      THEN calculated_deal_count 
      ELSE NULL 
    END                                                                                                   AS booked_deal_count_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
          AND is_eligible_open_pipeline = 1 
      THEN net_arr 
      ELSE NULL 
    END                                                                                                    AS open_1plus_net_arr_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
          AND is_eligible_open_pipeline = 1 
          AND base.is_stage_3_plus = 1 
      THEN net_arr 
      ELSE NULL 
    END                                                                                                    AS open_3plus_net_arr_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
          AND is_eligible_open_pipeline = 1 
          AND base.is_stage_4_plus = 1 
      THEN net_arr 
      ELSE NULL 
    END                                                                                                    AS open_4plus_net_arr_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
          AND is_eligible_open_pipeline = 1 
      THEN calculated_deal_count 
      ELSE NULL 
    END                                                                                                    AS open_1plus_deal_count_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
        AND is_eligible_open_pipeline = 1 
        AND base.is_stage_3_plus = 1 
      THEN calculated_deal_count 
      ELSE NULL 
    END                                                                                                    AS open_3plus_deal_count_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
          AND is_eligible_open_pipeline = 1 
          AND base.is_stage_4_plus = 1 
      THEN calculated_deal_count 
      ELSE NULL 
    END                                                                                                    AS open_4plus_deal_count_in_snapshot_quarter,
    -- Fields to calculate average deal size. Net arr in the numerator / deal count in the denominator
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
          AND is_booked_net_arr = TRUE 
          AND net_arr > 0 
      THEN 1 
      ELSE NULL 
    END                                                                                                    AS positive_booked_deal_count_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
          AND is_booked_net_arr = TRUE 
          AND net_arr > 0 
      THEN net_arr 
      ELSE NULL 
    END                                                                                                    AS positive_booked_net_arr_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
          AND is_eligible_open_pipeline = 1 
          AND net_arr > 0 
      THEN 1 
      ELSE NULL 
    END                                                                                                    AS positive_open_deal_count_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
          AND is_eligible_open_pipeline = 1 
          AND net_arr > 0 
      THEN net_arr 
      ELSE NULL 
    END                                                                                                    AS positive_open_net_arr_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
          AND base.is_closed = 'TRUE'
      THEN calculated_deal_count 
      ELSE NULL 
    END                                                                                                   AS closed_deals_in_snapshot_quarter,
    CASE 
      WHEN base.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
          AND base.is_closed = 'TRUE'
      THEN net_arr 
      ELSE NULL 
    END                                                                                                  AS closed_net_arr_in_snapshot_quarter,

    {{ get_date_id('base.close_date_live') }}                                                            AS close_date_live_date_id,
    close_date_live.fiscal_quarter_name_fy                                                               AS close_fiscal_quarter_name_live,                        

    -- This code calculates sales metrics without specific quarter alignment
    IFF(is_net_arr_pipeline_created = 1, net_arr, NULL)                                                  AS created_arr,
    IFF(is_closed_won = TRUE AND is_win_rate_calc = TRUE, calculated_deal_count, NULL)                   AS closed_won_opps,
    IFF(is_win_rate_calc = TRUE, calculated_deal_count, NULL)                                            AS closed_opps,
    IFF(is_net_arr_pipeline_created = 1, calculated_deal_count, NULL)                                    AS created_deals,
    -- Fields to calculate average deal size. Net arr in the numerator / deal count in the denominator
    IFF(is_booked_net_arr = TRUE AND net_arr > 0, 1, NULL)                                               AS positive_booked_deal_count,
    IFF(is_booked_net_arr = TRUE AND net_arr > 0, net_arr, NULL)                                         AS positive_booked_net_arr,
    IFF(is_eligible_open_pipeline = 1 AND net_arr > 0, 1, NULL)                                          AS positive_open_deal_count,
    IFF(is_eligible_open_pipeline = 1 AND net_arr > 0, net_arr, NULL)                                    AS positive_open_net_arr,
    IFF(base.is_closed = 'TRUE', calculated_deal_count, NULL)                                            AS closed_deals,
    IFF(base.is_closed = 'TRUE', net_arr, NULL)                                                          AS closed_net_arr,
    -- Updated is_live flag using window function, exclude deleted opportunities
    CASE 
      WHEN ROW_NUMBER() OVER (
          PARTITION BY base.dim_crm_opportunity_id 
          ORDER BY base.snapshot_date DESC
      ) = 1 
      AND base.is_deleted = FALSE
        THEN TRUE
      ELSE FALSE 
    END                                                                                                  AS is_live
  FROM base
  INNER JOIN sfdc_opportunity_stage
    ON base.stage_name = sfdc_opportunity_stage.primary_label
  LEFT JOIN quote
    ON base.dim_crm_opportunity_id = quote.dim_crm_opportunity_id
  LEFT JOIN opportunity_attribution_metrics
    ON base.dim_crm_opportunity_id = opportunity_attribution_metrics.dim_crm_opportunity_id
  LEFT JOIN first_contact
    ON base.dim_crm_opportunity_id = first_contact.opportunity_id
  LEFT JOIN dim_date AS close_date
    ON base.close_date = close_date.date_actual
  LEFT JOIN dim_date AS close_date_live
    ON base.close_date_live = close_date_live.date_actual
  LEFT JOIN dim_date AS created_date
    ON base.created_date = created_date.date_actual
  LEFT JOIN dim_date AS arr_created_date
    ON CAST(base.iacv_created_date AS DATE) = arr_created_date.date_actual
  LEFT JOIN dim_date AS subscription_start_date
    ON CAST(base.subscription_start_date AS DATE) = subscription_start_date.date_actual
  LEFT JOIN net_iacv_to_net_arr_ratio
    ON base.opportunity_owner_user_segment = net_iacv_to_net_arr_ratio.user_segment_stamped
      AND base.order_type = net_iacv_to_net_arr_ratio.order_type
  LEFT JOIN sfdc_record_type_source
    ON base.record_type_id = sfdc_record_type_source.record_type_id
  LEFT JOIN abm_tier_unioned
    ON base.dim_crm_opportunity_id = abm_tier_unioned.dim_crm_opportunity_id
)

SELECT *
FROM final
WHERE NOT is_deleted