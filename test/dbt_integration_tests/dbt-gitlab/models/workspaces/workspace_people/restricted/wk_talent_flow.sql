{{ simple_cte([
    ('employee_directory_intermediate','employee_directory_intermediate'),
    ('blended_employee_mapping_source', 'blended_employee_mapping_source'),
    ('employee_directory', 'employee_directory'),
    ('date_details', 'date_details'),
    ('bamboohr_employment_status_xf', 'bamboohr_employment_status_xf'),
    ('workday_terminations', 'workday_terminations'),
    ('staffing_history_approved_source', 'staffing_history_approved_source'),
    ('bamboohr_promotions_xf', 'bamboohr_promotions_xf'),
    ('bamboohr_directionary_bonuses_xf', 'bamboohr_directionary_bonuses_xf'),
    ('team_member_job_history', 'prep_team_member_job_history')
    ])
}},

 max_dir_date AS (

  SELECT
    MAX(date_actual)                                                         AS max_date,
    DATEADD('day', 31, DATEADD('year', 2, DATE_TRUNC('year', CURRENT_DATE))) AS cut_off_date
  FROM employee_directory_intermediate

),

eda_cur_max AS (

  SELECT eda_cur.*
  FROM employee_directory_intermediate AS eda_cur
  INNER JOIN max_dir_date 
    ON eda_cur.date_actual = max_dir_date.max_date
  WHERE is_termination_date = 'FALSE'

),

eda_fut AS (

  SELECT
    date_details.date_actual AS date_actual_true,
    eda_cur_max.*
  FROM date_details
  INNER JOIN max_dir_date
    ON date_details.date_actual BETWEEN max_dir_date.max_date + 1
      AND max_dir_date.cut_off_date
  LEFT JOIN eda_cur_max ON 1 = 1
  WHERE date_details.date_actual BETWEEN max_dir_date.max_date + 1
    AND max_dir_date.cut_off_date
    AND eda_cur_max.employee_id IS NOT NULL

),

job_role AS (

  SELECT *
  FROM blended_employee_mapping_source
  WHERE source_system = 'workday' QUALIFY ROW_NUMBER() OVER (
    PARTITION BY employee_id ORDER BY uploaded_at DESC
  ) = 1

),

hire_fut_stage AS (

  SELECT
    dir.*,
    job_role.job_role,
    job_role.job_grade
  FROM employee_directory AS dir
  LEFT JOIN max_dir_date ON 1 = 1
  LEFT JOIN job_role ON dir.employee_id = job_role.employee_id
  WHERE max_dir_date.max_date < dir.hire_date

),

hire_fut AS (

  SELECT
    date_details.date_actual,
    hire_fut_stage.*
  FROM date_details
  INNER JOIN max_dir_date
    ON date_actual BETWEEN max_dir_date.max_date
      AND max_dir_date.cut_off_date
  LEFT JOIN hire_fut_stage
    ON date_actual BETWEEN hire_date
      AND max_dir_date.cut_off_date
  WHERE hire_fut_stage.employee_id IS NOT NULL

),

eda_all AS (

  SELECT
    date_actual,
    employee_id,
    full_name,
    country,
    CASE region_modified
      WHEN 'NORAM'
        THEN 'Americas'
      WHEN 'JAPAC' 
        THEN 'APJ'    
      ELSE region_modified
    END AS region,
    cost_center,
    division,
    department,
    job_title,
    job_role,
    job_grade,
    reports_to,
    is_promotion
  FROM employee_directory_intermediate

  UNION ALL

  SELECT
    date_actual_true,
    employee_id,
    full_name,
    country,
    CASE region_modified
      WHEN 'NORAM'
        THEN 'Americas'
      WHEN 'JAPAC' 
        THEN 'APJ'  
      ELSE region_modified
    END AS region,
    cost_center,
    division,
    department,
    job_title,
    job_role,
    job_grade,
    reports_to,
    is_promotion
  FROM eda_fut

  UNION ALL

  SELECT
    date_actual,
    employee_id,
    full_name,
    country,
    CASE region_modified
      WHEN 'NORAM'
        THEN 'Americas'
      WHEN 'JAPAC' 
        THEN 'APJ'    
      ELSE region_modified
    END     AS region,
    last_cost_center,
    last_division,
    last_department,
    last_job_title,
    job_role,
    job_grade,
    last_supervisor,
    'FALSE' AS is_promotion
  FROM hire_fut

),

start_date_stage AS (

  SELECT
    employee_id AS hire_id,
    date_actual AS hire_date
  FROM employee_directory_intermediate
  WHERE is_hire_date = 'TRUE'

  UNION

  SELECT
    employee_id AS hire_id,
    hire_date
  FROM employee_directory

),

start_date AS (

  SELECT
    hire_id,
    hire_date,
    ROW_NUMBER() OVER (
      PARTITION BY hire_id ORDER BY hire_date ASC
    ) AS hire_rank
  FROM start_date_stage

),

end_date_stage AS (

  SELECT
    employee_id AS term_id,
    date_actual AS term_date
  FROM employee_directory_intermediate
  WHERE is_termination_date = 'TRUE'

  UNION ALL

  SELECT
    employee_id     AS term_id,
    valid_from_date AS term_date
  FROM bamboohr_employment_status_xf
  LEFT JOIN max_dir_date ON 1 = 1
  WHERE employment_status = 'Terminated'
    AND valid_from_date BETWEEN max_date
    AND cut_off_date

),

end_date AS (

  SELECT DISTINCT
    term_id,
    term_date,
    ROW_NUMBER() OVER (
      PARTITION BY term_id ORDER BY term_date ASC
    ) AS term_rank
  FROM end_date_stage
  
),

start_to_end AS (

  SELECT
    hire_id,
    hire_rank,
    hire_date,
    term_date,
    term_rank,
    COALESCE(term_date, DATEADD('day', 31, DATEADD('year', 3, DATE_TRUNC('year', CURRENT_DATE)))) AS last_date
  FROM start_date
  LEFT JOIN end_date ON hire_id = term_id
    AND hire_rank = term_rank

),

listing_stage AS (

  SELECT
    eda_all.*,
    start_to_end.hire_date,
    start_to_end.hire_rank,
    start_to_end.term_date,
    start_to_end.term_rank,
    start_to_end.last_date,
    CASE
      WHEN eda_all.date_actual = start_to_end.hire_date
        THEN 'True'
      ELSE 'False'
    END AS is_hire_date,
    CASE
      WHEN eda_all.date_actual = start_to_end.term_date
        THEN 'True'
      ELSE 'False'
    END AS is_termination_date
  FROM eda_all
  INNER JOIN start_to_end ON employee_id = hire_id
    AND date_actual BETWEEN hire_date
    AND COALESCE(term_date, last_date)

),

term_type AS (

  SELECT
    xf.employee_id       AS type_id,
    xf.employment_status AS type_status,
    xf.termination_type  AS type_termination_type,
    xf.valid_from_date   AS type_date,
    ROW_NUMBER() OVER (
      PARTITION BY
        xf.employee_id,
        type_date
      ORDER BY type_date DESC
    )                    AS type_rank,
    term.termination_reason,
    term.exit_impact,
    term.initiated_at    AS termination_initiated_at
  FROM bamboohr_employment_status_xf AS xf
  LEFT JOIN workday_terminations AS term
    ON xf.employee_id = term.employee_id
      AND xf.valid_from_date = term.effective_date
  WHERE xf.employment_status = 'Terminated'

),

staff_hist AS (

  SELECT
    *,
    COALESCE(LAG(effective_date) OVER (
      PARTITION BY
        employee_id ORDER BY effective_date DESC,
      date_time_initiated DESC
    ), '2099-01-01') AS next_effective_date
  FROM staffing_history_approved_source

),

job_hist AS (

  SELECT
   *,
   COALESCE(LAG(valid_from) OVER (
     PARTITION BY 
       employee_id ORDER BY valid_from DESC,
       valid_to DESC), '2099-01-01') as next_effective_date
  FROM team_member_job_history

),

transfer AS (

  SELECT 
    employee_id,
    effective_date,
    MAX(business_process_reason)                                                               AS business_process_reason,
    MAX(IFF(COALESCE(job_code_past, '1') != COALESCE(job_code_current, '2'), 'TRUE', 'FALSE')) AS transfer_job_change
  FROM staff_hist
  WHERE business_process_category = 'Lateral Move'
  GROUP BY 1, 2

),

promo AS (

  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        employee_id,
        promotion_date
      ORDER BY promotion_date DESC
    ) AS promo_rank
  FROM bamboohr_promotions_xf 
  QUALIFY promo_rank = 1

),

staff_hist_promo AS (

  SELECT DISTINCT
    employee_id,
    effective_date,
    business_process_type
  FROM staff_hist
  WHERE business_process_type = 'Promote Employee Inbound'

),

bonus AS (

  SELECT
    employee_id,
    bonus_date,
    total_discretionary_bonuses
  FROM bamboohr_directionary_bonuses_xf

),

listing AS (

  SELECT
    listing_stage.date_actual,
    listing_stage.employee_id,
    listing_stage.reports_to,
    listing_stage.full_name,
    CASE COALESCE(staff_hist.country_current, listing_stage.country)
      WHEN 'United States' THEN 'United States of America'
      ELSE COALESCE(staff_hist.country_current, listing_stage.country)
    END                                                                                                          AS country,
    CASE COALESCE(staff_hist.region_current, listing_stage.region)
      WHEN 'NORAM' THEN 'Americas'    
      WHEN 'JAPAC' THEN 'APJ'
    ELSE COALESCE(staff_hist.region_current, listing_stage.region)
    END                                                                                                          AS region,
    listing_stage.cost_center,
    CASE listing_stage.division
      WHEN 'Legal'
        THEN 'LACA'
      ELSE listing_stage.division
    END                                                                                                          AS division,
    CASE
      WHEN listing_stage.department IN (
          'TAM (inactive)',
          'TAM'
        )
        THEN 'CSM'
      ELSE listing_stage.department
    END                                                                                                          AS department,
    COALESCE(job_hist.job_title, listing_stage.job_title)                                                        AS job_title,
    COALESCE(job_hist.business_title, listing_stage.job_title)                                                   AS business_title,
    job_hist.job_profile_workday_id                                                                              AS job_id,
    job_hist.job_family,
    coalesce(job_hist.job_role, listing_stage.job_role)                                                          AS job_role,
    coalesce(job_hist.job_grade, listing_stage.job_grade)                                                        AS job_grade,
    listing_stage.is_hire_date,
    listing_stage.is_termination_date,
    ''                                                                                                           AS termination_type,
    ''                                                                                                           AS termination_reason,
    ''                                                                                                           AS exit_impact,
    NULL                                                                                                         AS termination_initiated_at,
    listing_stage.hire_date,
    listing_stage.hire_rank,
    listing_stage.term_date,
    listing_stage.term_rank,
    listing_stage.last_date,
    IFF(staff_hist_promo.business_process_type = 'Promote Employee Inbound', 'true', listing_stage.is_promotion) AS is_promotion,
    IFF(LEFT(transfer.business_process_reason,8) = 'Transfer', 'TRUE', 'FALSE')                                  AS is_transfer,
    COALESCE(transfer.transfer_job_change, 'FALSE')                                                              AS transfer_job_change,
    staff_hist.employee_type_current                                                                             AS employee_type,
    bonus.total_discretionary_bonuses
  FROM listing_stage
  LEFT JOIN staff_hist_promo
    ON listing_stage.employee_id = staff_hist_promo.employee_id
      AND listing_stage.date_actual = staff_hist_promo.effective_date
  LEFT JOIN transfer
    ON listing_stage.employee_id = transfer.employee_id
      AND listing_stage.date_actual = transfer.effective_date
  LEFT JOIN staff_hist
    ON listing_stage.employee_id = staff_hist.employee_id
      AND listing_stage.date_actual >= staff_hist.effective_date
      AND listing_stage.date_actual < staff_hist.next_effective_date
      AND '2022-06-16' <= listing_stage.date_actual
  LEFT JOIN job_hist
    ON listing_stage.employee_id = job_hist.employee_id
      AND listing_stage.date_actual >= job_hist.valid_from
      AND listing_stage.date_actual < job_hist.next_effective_date      
  LEFT JOIN bonus
    ON listing_stage.employee_id = bonus.employee_id
      AND listing_stage.date_actual = bonus.bonus_date     

  UNION

  SELECT
    listing_stage.date_actual + 1,
    listing_stage.employee_id,
    listing_stage.reports_to,
    listing_stage.full_name,
    CASE COALESCE(staff_hist.country_current, listing_stage.country)
      WHEN 'United States' THEN 'United States of America'
      ELSE COALESCE(staff_hist.country_current, listing_stage.country)
    END                                                                                                          AS country,
    CASE COALESCE(staff_hist.region_current, listing_stage.region)
      WHEN 'NORAM' THEN 'Americas'
      WHEN 'JAPAC' THEN 'APJ'
    ELSE COALESCE(staff_hist.region_current, listing_stage.region)
    END                                                                                                          AS region,
    listing_stage.cost_center,
    CASE listing_stage.division
      WHEN 'Legal'
        THEN 'LACA'
      ELSE listing_stage.division
    END                                                                                                          AS division,
    CASE
      WHEN listing_stage.department IN (
          'TAM (inactive)',
          'TAM'
        )
        THEN 'CSM'
      ELSE listing_stage.department
    END                                                                                                          AS department,
    COALESCE(job_hist.job_title, listing_stage.job_title)                                                        AS job_title,
    COALESCE(job_hist.business_title, listing_stage.job_title)                                                   AS business_title,
    job_hist.job_profile_workday_id                                                                              AS job_id,
    job_hist.job_family,
    COALESCE(job_hist.job_role, listing_stage.job_role)                                                          AS job_role,
    COALESCE(job_hist.job_grade, listing_stage.job_grade)                                                        AS job_grade,
    listing_stage.is_hire_date,
    listing_stage.is_termination_date,
    term_type.type_termination_type                                                                              AS termination_type,
    term_type.termination_reason,
    CASE term_type.exit_impact
      WHEN 'Yes'
        THEN 'Regrettable'
      WHEN 'No'
        THEN 'Non-Regrettable'
      ELSE term_type.exit_impact
    END                                                                                                          AS exit_impact,
    term_type.termination_initiated_at,
    listing_stage.hire_date,
    listing_stage.hire_rank,
    listing_stage.term_date,
    listing_stage.term_rank,
    listing_stage.last_date,
    IFF(staff_hist_promo.business_process_type = 'Promote Employee Inbound', 'true', listing_stage.is_promotion) AS is_promotion,
    IFF(LEFT(transfer.business_process_reason,8) = 'Transfer', 'TRUE', 'FALSE')                                  AS is_transfer,
    COALESCE(transfer.transfer_job_change, 'FALSE')                                                              AS transfer_job_change,
    staff_hist.employee_type_current                                                                             AS employee_type,
    bonus.total_discretionary_bonuses
  FROM listing_stage
  LEFT JOIN term_type
    ON listing_stage.employee_id = type_id
      AND listing_stage.date_actual = type_date
      AND 1 = type_rank
  LEFT JOIN staff_hist_promo
    ON listing_stage.employee_id = staff_hist_promo.employee_id
      AND listing_stage.date_actual = staff_hist_promo.effective_date
  LEFT JOIN transfer
    ON listing_stage.employee_id = transfer.employee_id
      AND listing_stage.date_actual = transfer.effective_date
  LEFT JOIN staff_hist
    ON listing_stage.employee_id = staff_hist.employee_id
      AND listing_stage.date_actual >= staff_hist.effective_date
      AND listing_stage.date_actual < staff_hist.next_effective_date
      AND '2022-06-16' <= listing_stage.date_actual
  LEFT JOIN job_hist
    ON listing_stage.employee_id = job_hist.employee_id
      AND listing_stage.date_actual >= job_hist.valid_from
      AND listing_stage.date_actual < job_hist.next_effective_date        
  LEFT JOIN bonus
    ON listing_stage.employee_id = bonus.employee_id
      AND listing_stage.date_actual = bonus.bonus_date       
  WHERE is_termination_date = 'True'

),

pr AS (

  SELECT
    employee_id   AS pr_id,
    date_actual   AS pr_date_actual,
    reports_to    AS pr_reports_to,
    country       AS pr_country,
    region        AS pr_region,
    cost_center   AS pr_cost_center,
    job_title     AS pr_job_title,
    job_role      AS pr_job_role,
    job_grade     AS pr_job_grade,    
    department    AS pr_department,
    division      AS pr_division,
    employee_type AS pr_employee_type,
    hire_rank     AS pr_hire_rank
  FROM listing

),

hist_stage AS (

  SELECT
    employee_id         AS cur_id,
    full_name,
    date_actual         AS cur_date_actual,
    CASE
      WHEN date_actual = term_date + 1
        THEN 'T'
      ELSE 'A'
    END                 AS employment_status,
    CASE
      WHEN employment_status = 'T'
        THEN cur_date_actual
      ELSE COALESCE(LAG(cur_date_actual) OVER (
          PARTITION BY employee_id, hire_rank ORDER BY cur_date_actual DESC
        ) - 1, last_date)
    END                 AS end_date,
    reports_to          AS cur_reports_to,
    country             AS cur_country,
    region              AS cur_region,
    cost_center         AS cur_cost_center,
    job_title           AS cur_job_title,
    job_role            AS cur_job_role,
    job_grade           AS cur_job_grade,    
    department          AS cur_department,
    division            AS cur_division,
    hire_date,
    hire_rank,
    last_date,
    term_date,
    term_rank,
    termination_type,
    termination_reason,
    exit_impact,
    termination_initiated_at,
    employee_type       AS cur_employee_type,
    is_promotion        AS cur_is_promotion,
    is_transfer         AS cur_is_transfer,
    transfer_job_change AS cur_transfer_job_change,
    total_discretionary_bonuses,
    pr.*,
    CASE
      WHEN hire_date = cur_date_actual
        THEN 1
      WHEN term_date + 1 = date_actual
        THEN 1
      WHEN COALESCE(cur_reports_to, '1') != COALESCE(pr_reports_to, '1')
        THEN 1
      WHEN COALESCE(cur_country, '1') != COALESCE(pr_country, '1')
        THEN 1
      WHEN COALESCE(cur_region, '1') != COALESCE(pr_region, '1')
        THEN 1
      WHEN COALESCE(cur_cost_center, '1') != COALESCE(pr_cost_center, '1')
        THEN 1
      WHEN COALESCE(cur_job_title, '1') != COALESCE(pr_job_title, '1')
        THEN 1
      WHEN COALESCE(cur_department, '1') != COALESCE(pr_department, '1')
        THEN 1
      WHEN COALESCE(cur_division, '1') != COALESCE(pr_division, '1')
        THEN 1
      WHEN COALESCE(cur_job_role, '1') != COALESCE(pr_job_role, '1')
        THEN 1
      WHEN COALESCE(cur_job_grade, '1') != COALESCE(pr_job_grade, '1')
        THEN 1         
      WHEN COALESCE(cur_employee_type, '1') != COALESCE(pr_employee_type, '1')
        THEN 1
      WHEN cur_is_promotion = 'true'
        THEN 1
      WHEN cur_is_transfer = 'TRUE'
        AND cur_transfer_job_change = 'TRUE'
        THEN 1
      WHEN total_discretionary_bonuses >= 1
        THEN 1
      ELSE 0
    END                 AS filter
  FROM listing
  LEFT JOIN pr
    ON cur_id = pr_id
      AND cur_date_actual - 1 = pr_date_actual
      AND hire_rank = pr_hire_rank
  WHERE filter = 1
  ORDER BY 1, 2 DESC


),

final AS (

  SELECT
    hist_stage.cur_id                        AS employee_id,
    hist_stage.full_name,
    hist_stage.employment_status,
    hist_stage.cur_date_actual               AS min_date,
    hist_stage.end_date                      AS max_date,
    CASE
      WHEN hist_stage.hire_date = min_date
        AND hist_stage.hire_rank = 1
        THEN 'Hire'
      WHEN hist_stage.hire_date = min_date
        AND hire_rank > 1
        THEN 'Rehire'
      WHEN hist_stage.employment_status = 'T'
        THEN 'Termination'
      WHEN cur_is_promotion = 'true'
        OR min_date = promotion_date::DATE
        THEN 'Promotion'
      WHEN cur_is_transfer = 'TRUE'
        AND cur_transfer_job_change = 'TRUE'
        THEN 'Transfer'
      WHEN COALESCE(hist_stage.cur_employee_type, '1') != COALESCE(pr_employee_type, '1')
        THEN 'Employee Type Change'
      WHEN COALESCE(hist_stage.cur_country, '1') != COALESCE(pr_country, '1')
        THEN 'Organization Change'
      WHEN COALESCE(hist_stage.cur_region, '1') != COALESCE(pr_region, '1')
        THEN 'Organization Change'
      WHEN COALESCE(hist_stage.cur_cost_center, '1') != COALESCE(pr_cost_center, '1')
        THEN 'Organization Change'
      WHEN COALESCE(hist_stage.cur_division, '1') != COALESCE(pr_division, '1')
        THEN 'Organization Change'
      WHEN COALESCE(hist_stage.cur_department, '1') != COALESCE(pr_department, '1')
        THEN 'Organization Change'
      WHEN COALESCE(hist_stage.cur_job_title, '1') != COALESCE(pr_job_title, '1')
        THEN 'Job Title Change'
      WHEN COALESCE(hist_stage.cur_job_role, '1') != COALESCE(pr_job_role, '1')
        THEN 'Job Role Change'
      WHEN COALESCE(hist_stage.cur_job_grade, '1') != COALESCE(pr_job_grade, '1')
        THEN 'Job Grade Change'         
      WHEN COALESCE(hist_stage.cur_reports_to, '1') != COALESCE(pr_reports_to, '1')
        THEN 'Supervisor Change'
      WHEN hist_stage.total_discretionary_bonuses >= 1
        THEN 'Discretionary Bonus'
    END                                      AS job_change_reason,
    hist_stage.cur_country                   AS country,
    hist_stage.cur_region                    AS region,
    hist_stage.cur_cost_center               AS cost_center,
    hist_stage.cur_division                  AS division,
    hist_stage.cur_department                AS department,
    hist_stage.cur_job_title                 AS job_title,
    hist_stage.cur_job_role                  AS job_role,
    hist_stage.cur_job_grade                 AS job_grade,    
    hist_stage.cur_reports_to                AS reports_to,
    hist_stage.hire_date,
    hist_stage.hire_rank,
    hist_stage.last_date,
    hist_stage.term_date + 1                 AS termination_date,
    hist_stage.term_rank,
    hist_stage.termination_type,
    hist_stage.termination_reason,
    hist_stage.exit_impact,
    hist_stage.termination_initiated_at,
    hist_stage.cur_employee_type             AS employee_type,
    COALESCE(total_discretionary_bonuses, 0) AS discretionary_bonus_count
  FROM hist_stage
  LEFT JOIN bamboohr_promotions_xf AS promo
    ON hist_stage.cur_id = promo.employee_number
      AND hist_stage.cur_date_actual = promo.promotion_date::DATE
      AND 1 = promo.compensation_sequence
  WHERE NOT COALESCE(hist_stage.cur_employee_type, '') IN ('Intern (Trainee)')
  ORDER BY hist_stage.cur_id ASC, min_date DESC

)

SELECT *
FROM final
