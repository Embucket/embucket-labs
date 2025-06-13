WITH dim_date AS (

  SELECT * FROM {{ ref('dim_date') }}

),

accounts_with_opps_boq AS (

  SELECT DISTINCT
    snap.snapshot_fiscal_quarter_name AS snap_quarter,
    snapshot_date,
    CONCAT(snap.dim_crm_account_id, snap.close_fiscal_quarter_name) AS concatenation
  FROM {{ ref('mart_crm_opportunity_daily_snapshot') }} AS snap
  LEFT JOIN dim_date
    ON snap.snapshot_date = dim_date.date_actual
  WHERE dim_date.is_third_business_day_of_fiscal_quarter = TRUE 
    AND snapshot_fiscal_quarter_date = close_fiscal_quarter_date
    AND snap.is_eligible_open_pipeline = TRUE
    AND (snap.order_type LIKE ('%1%') OR snap.order_type LIKE ('%2%') OR snap.order_type LIKE ('%3%'))
    AND LOWER(opportunity_name) NOT LIKE '%bad debt%'
    AND LOWER(opportunity_name) NOT LIKE '% bd %'
    AND LOWER(opportunity_name) NOT LIKE '% bdd %'
    AND order_type NOT LIKE '%4%'
    AND order_type NOT LIKE '%5%'
    AND order_type NOT LIKE '%6%'
    AND opportunity_category NOT LIKE '%Decom%'
    AND opportunity_category NOT LIKE '%Internal%'

),

accounts_with_future_opps AS (

  SELECT DISTINCT
    snapshot_fiscal_quarter_name AS snap_quarter,
    dim_crm_account_id,
    CONCAT(snap.dim_crm_account_id, snap.snapshot_fiscal_quarter_name) AS concatenation
  FROM {{ ref('mart_crm_opportunity_daily_snapshot') }} AS snap
  LEFT JOIN dim_date
    ON snap.snapshot_date = dim_date.date_actual
  WHERE dim_date.is_third_business_day_of_fiscal_quarter = TRUE 
    AND close_fiscal_quarter_date > snapshot_fiscal_quarter_date
    AND snap.is_eligible_open_pipeline = TRUE
    AND (snap.order_type LIKE ('%1%') OR snap.order_type LIKE ('%2%') OR snap.order_type LIKE ('%3%'))
    AND LOWER(opportunity_name) NOT LIKE '%bad debt%'
    AND LOWER(opportunity_name) NOT LIKE '% bd %'
    AND LOWER(opportunity_name) NOT LIKE '% bdd %'
    AND order_type NOT LIKE '%4%'
    AND order_type NOT LIKE '%5%'
    AND order_type NOT LIKE '%6%'
    AND opportunity_category NOT LIKE '%Decom%'
    AND opportunity_category NOT LIKE '%Internal%'

),

dates_less_equal_third_biz_day AS (

  SELECT DISTINCT
    first_day_of_fiscal_quarter,
    date_actual AS third_day
  FROM dim_date
  WHERE dim_date.is_third_business_day_of_fiscal_quarter = TRUE

),

all_booked_accounts AS (

  SELECT
    close_fiscal_quarter_name,
    close_fiscal_quarter_date,
    dim_crm_opportunity_id,
    dim_crm_account_id,
    net_arr AS bookings,
    order_type,
    opportunity_category,
    report_role_level_1 AS role_level_1,
    report_role_level_2 AS role_level_2,
    report_role_level_3 AS role_level_3,
    close_date,
    CONCAT(dim_crm_account_id, close_fiscal_quarter_name) AS concatenation,
    COALESCE(
      stage_name = 'Closed Won'
      AND (
        LOWER(opportunity_name) LIKE '%bad debt%'
        OR LOWER(opportunity_name) LIKE '% bd %'
        OR LOWER(opportunity_name) LIKE '% bdd %'
      ),
      FALSE
    ) AS bad_debt_flag,
    COALESCE(
      dim_date.first_day_of_fiscal_quarter = o.close_fiscal_quarter_date 
      AND o.close_date <= d.third_day, FALSE
    ) AS already_booked_flag
  FROM {{ ref('mart_crm_opportunity') }} AS o
  LEFT JOIN dim_date
    ON o.close_date = dim_date.date_actual
  LEFT JOIN dates_less_equal_third_biz_day AS d
       ON o.close_fiscal_quarter_date = d.first_day_of_fiscal_quarter
  WHERE fpa_master_bookings_flag = TRUE

),

final AS (

  SELECT
    all_accs.dim_crm_opportunity_id,
    all_accs.dim_crm_account_id,
    all_accs.close_fiscal_quarter_name,
    all_accs.close_fiscal_quarter_date,
    all_accs.role_level_1,
    all_accs.role_level_2,
    all_accs.role_level_3,
    CASE
      WHEN all_accs.bad_debt_flag = TRUE THEN 'Bad Debt'
      WHEN all_accs.order_type LIKE ANY ('%4%', '%5%', '%6%')
        THEN 'Churn/Contraction'
      WHEN all_accs.opportunity_category LIKE ANY ('%Decom%', '%Internal%')
        THEN 'Accounting'
      WHEN all_accs.already_booked_flag = TRUE THEN 'Already Booked'
      WHEN all_accs.concatenation IN (
        SELECT DISTINCT concatenation
        FROM accounts_with_opps_boq
      ) THEN 'Starting'
      WHEN all_accs.concatenation IN (
        SELECT DISTINCT concatenation
        FROM accounts_with_future_opps
      ) THEN 'Pulled Forward'
      ELSE 'Created and Closed'
    END AS bucket,
    all_accs.bookings
  FROM all_booked_accounts AS all_accs
  LEFT JOIN accounts_with_opps_boq AS starting
    ON all_accs.concatenation = starting.concatenation
  LEFT JOIN accounts_with_future_opps AS pulled_f
    ON all_accs.dim_crm_account_id = pulled_f.dim_crm_account_id
    AND all_accs.close_fiscal_quarter_name = pulled_f.snap_quarter

)

SELECT *
FROM final
