WITH live_data AS (

  SELECT *
  FROM {{ ref('mart_crm_opportunity') }}
),

snapshot_data AS (

  SELECT *
  FROM {{ ref('mart_crm_opportunity_daily_snapshot') }}

),

base AS (
  SELECT --create date and close date
    dim_crm_opportunity_id,
    CASE WHEN stage_name = '7 - Closing' THEN '7-Closing'
      WHEN stage_name = 'Closed Lost' THEN '8-Closed Lost'
      ELSE stage_name
    END                AS stage_name,
    created_date,
    -- Some opptys should be excluded from reporting
    MIN(snapshot_date) AS stage_date
  FROM snapshot_data
  GROUP BY ALL
),

reporting_eligibility AS (
  SELECT
    dim_crm_opportunity_id,
    CASE
      WHEN (is_eligible_open_pipeline = 1 OR LOWER(stage_name) LIKE '%close%')
        AND sales_qualified_source_name != 'Web Direct Generated'
        AND created_date >= '2020-02-01'
        -- AND subscription_type != 'Renewal'
        AND is_web_portal_purchase = FALSE
        -- AND opportunity_category NOT IN ('Decommission', 'Internal Correction')
        -- AND LOWER(opportunity_name) NOT LIKE '%rebook%'
        -- AND net_arr > 0
        THEN 1
      ELSE 0
    END AS is_eligible_reporting
  FROM live_data
),

stage_base AS (
  SELECT
    dim_crm_opportunity_id,
    CASE WHEN stage_name = '0-Pending Acceptance' THEN 'stage0'
      WHEN stage_name = '1-Discovery' THEN 'stage1'
      WHEN stage_name = '2-Scoping' THEN 'stage2'
      WHEN stage_name = '3-Technical Evaluation' THEN 'stage3'
      WHEN stage_name = '4-Proposal' THEN 'stage4'
      WHEN stage_name = '5-Negotiating' THEN 'stage5'
      WHEN stage_name = '6-Awaiting Signature' THEN 'stage6'
      WHEN stage_name = '7-Closing' THEN 'stage7'
      WHEN stage_name = '8-Closed Lost' THEN 'closed_lost'
      WHEN stage_name = 'Closed Won' THEN 'closed_won'
    END                                                                                 AS stage_name,
    CASE
      WHEN stage_name = '0-Pending Acceptance' THEN 0
      WHEN stage_name = '1-Discovery' THEN 1
      WHEN stage_name = '2-Scoping' THEN 2
      WHEN stage_name = '3-Technical Evaluation' THEN 3
      WHEN stage_name = '4-Proposal' THEN 4
      WHEN stage_name = '5-Negotiating' THEN 5
      WHEN stage_name = '6-Awaiting Signature' THEN 6
      WHEN stage_name = '7-Closing' THEN 7
      WHEN stage_name IN ('8-Closed Lost', 'Closed Won') THEN 8
    END                                                                                 AS stage_number,
    LAG(stage_number, 1) OVER (PARTITION BY dim_crm_opportunity_id ORDER BY stage_date) AS prev_stage_number,
    -- 1 if the opportunity regresses in stage, 0 otherwise
    CASE
      WHEN prev_stage_number IS NOT NULL AND prev_stage_number > stage_number THEN 1
      ELSE 0
    END                                                                                 AS is_stage_regression,
    created_date,
    stage_date
  FROM base
  WHERE stage_name IN ('0-Pending Acceptance', '1-Discovery', '2-Scoping', '3-Technical Evaluation', '4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing', '8-Closed Lost', 'Closed Won')
),

flag_stages AS (
  SELECT
    dim_crm_opportunity_id,
    MAX(is_stage_regression) AS is_stage_regression
  FROM stage_base
  GROUP BY dim_crm_opportunity_id
),

stage_pivot AS (
  SELECT *
  FROM (SELECT
    dim_crm_opportunity_id,
    created_date,
    stage_name,
    stage_date
  FROM stage_base)
  PIVOT (MAX(stage_date) FOR stage_name IN ('stage0', 'stage1', 'stage2', 'stage3', 'stage4', 'stage5', 'stage6', 'stage7', 'closed_lost', 'closed_won'))
),

stage_dates AS (
  SELECT
    piv.dim_crm_opportunity_id,
    CASE WHEN piv."'closed_lost'" IS NOT NULL THEN 'Lost'
      WHEN piv."'closed_won'" IS NOT NULL THEN 'Won'
      ELSE 'Open'
    END                                                                AS stage_category,
    piv.created_date,
    piv."'stage0'"                                                     AS stage0_date,
    piv."'stage1'"                                                     AS stage1_date,
    piv."'stage2'"                                                     AS stage2_date,
    piv."'stage3'"                                                     AS stage3_date,
    piv."'stage4'"                                                     AS stage4_date,
    piv."'stage5'"                                                     AS stage5_date,
    piv."'stage6'"                                                     AS stage6_date,
    piv."'stage7'"                                                     AS stage7_date,
    COALESCE(piv."'closed_lost'", piv."'closed_won'", live_data.close_date) AS close_date
  FROM stage_pivot AS piv
  LEFT JOIN live_data
    ON piv.dim_crm_opportunity_id = live_data.dim_crm_opportunity_id
),

dates_adj AS (
  SELECT
    stage_dates.dim_crm_opportunity_id,
    stage_dates.stage_category,
    stage_dates.created_date,
    COALESCE(stage_dates.stage0_date, stage_dates.created_date) AS stage0_date,
    CASE WHEN stage_dates.stage_category = 'Open' AND stage_dates.stage1_date IS NULL 
        THEN COALESCE(stage_dates.stage2_date, stage_dates.stage3_date, stage_dates.stage4_date, stage_dates.stage5_date, stage_dates.stage6_date, stage_dates.stage7_date)
      ELSE COALESCE(stage_dates.stage1_date, stage_dates.stage2_date, stage_dates.stage3_date, stage_dates.stage4_date, stage_dates.stage5_date, stage_dates.stage6_date, stage_dates.stage7_date, stage_dates.close_date)
    END                                                         AS stage1_date,
    CASE WHEN stage_dates.stage_category = 'Open' AND stage_dates.stage2_date IS NULL 
        THEN COALESCE(stage_dates.stage3_date, stage_dates.stage3_date, stage_dates.stage4_date, stage_dates.stage5_date, stage_dates.stage6_date, stage_dates.stage7_date)
      ELSE COALESCE(stage_dates.stage2_date, stage_dates.stage3_date, stage_dates.stage4_date, stage_dates.stage5_date, stage_dates.stage6_date, stage_dates.stage7_date, stage_dates.close_date)
    END                                                         AS stage2_date,
    CASE WHEN stage_dates.stage_category = 'Open' AND stage_dates.stage3_date IS NULL 
        THEN COALESCE(stage_dates.stage4_date, stage_dates.stage5_date, stage_dates.stage6_date, stage_dates.stage7_date)
      ELSE COALESCE(stage_dates.stage3_date, stage_dates.stage4_date, stage_dates.stage5_date, stage_dates.stage6_date, stage_dates.stage7_date, stage_dates.close_date)
    END                                                         AS stage3_date,
    CASE WHEN stage_dates.stage_category = 'Open' AND stage_dates.stage4_date IS NULL 
        THEN COALESCE(stage_dates.stage5_date, stage_dates.stage6_date, stage_dates.stage7_date)
      ELSE COALESCE(stage_dates.stage4_date, stage_dates.stage5_date, stage_dates.stage6_date, stage_dates.stage7_date, stage_dates.close_date)
    END                                                         AS stage4_date,
    CASE WHEN stage_dates.stage_category = 'Open' AND stage_dates.stage5_date IS NULL 
        THEN COALESCE(stage_dates.stage6_date, stage_dates.stage7_date)
      ELSE COALESCE(stage_dates.stage5_date, stage_dates.stage6_date, stage_dates.stage7_date, stage_dates.close_date)
    END                                                         AS stage5_date,
    CASE WHEN stage_dates.stage_category = 'Open' AND stage_dates.stage6_date IS NULL
        THEN stage_dates.stage7_date
      ELSE COALESCE(stage_dates.stage6_date, stage_dates.stage7_date, stage_dates.close_date)
    END                                                         AS stage6_date,
    CASE WHEN stage_dates.stage_category = 'Open' AND stage_dates.stage7_date IS NULL
        THEN stage_dates.stage7_date
      ELSE COALESCE(stage_dates.stage7_date, stage_dates.close_date)
    END                                                         AS stage7_date,
    stage_dates.close_date,
    flag_stages.is_stage_regression,
    reporting_eligibility.is_eligible_reporting
  FROM stage_dates
  LEFT JOIN flag_stages
    ON stage_dates.dim_crm_opportunity_id = flag_stages.dim_crm_opportunity_id
  LEFT JOIN reporting_eligibility
    ON stage_dates.dim_crm_opportunity_id = reporting_eligibility.dim_crm_opportunity_id
),

opp_snap AS (
  SELECT
    dim_crm_opportunity_id,
    stage_category,
    created_date,
    stage0_date - created_date                                                        AS create_days,
    stage0_date,
    -- Calculate days between stages; set to 0 if negative to handle unexpected stage transitions 
    CASE WHEN stage1_date - stage0_date < 0 THEN 0 ELSE stage1_date - stage0_date END AS stage0_days,
    stage1_date,
    CASE WHEN stage2_date - stage1_date < 0 THEN 0 ELSE stage2_date - stage1_date END AS stage1_days,
    stage2_date,
    CASE WHEN stage3_date - stage2_date < 0 THEN 0 ELSE stage3_date - stage2_date END AS stage2_days,
    stage3_date,
    CASE WHEN stage4_date - stage3_date < 0 THEN 0 ELSE stage4_date - stage3_date END AS stage3_days,
    stage4_date,
    CASE WHEN stage5_date - stage4_date < 0 THEN 0 ELSE stage5_date - stage4_date END AS stage4_days,
    stage5_date,
    CASE WHEN stage6_date - stage5_date < 0 THEN 0 ELSE stage6_date - stage5_date END AS stage5_days,
    stage6_date,
    CASE WHEN stage7_date - stage6_date < 0 THEN 0 ELSE stage7_date - stage6_date END AS stage6_days,
    stage7_date,
    CASE WHEN close_date - stage7_date < 0 THEN 0 ELSE close_date - stage7_date END   AS stage7_days,
    close_date,
    CASE
      WHEN stage_category = 'Open'
        THEN CURRENT_DATE() - COALESCE(stage7_date, stage6_date, stage5_date, stage4_date, stage3_date, stage2_date, stage1_date, stage0_date, created_date)
    END                                                                               AS current_days,
    is_stage_regression,
    is_eligible_reporting
  FROM dates_adj
)

SELECT * FROM opp_snap