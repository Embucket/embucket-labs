WITH blended_directory_source AS (

    SELECT *,
      uploaded_at::date                                                     AS uploaded_date
    FROM {{ ref('blended_directory_source') }}

),

eda_stage AS (
    
  SELECT *
  FROM {{ ref('employee_directory_intermediate') }} 
  QUALIFY ROW_NUMBER() OVER ( PARTITION BY employee_id, date_actual ORDER BY date_actual DESC ) = 1
  ORDER BY date_actual DESC 

),

start_to_end AS (

  SELECT *
  FROM {{ ref('prep_team_member_history') }}

),

term_type AS (
  
  SELECT   
    employee_id                                                                       AS type_id,
    employment_status                                                                 AS type_status,
    termination_type                                                                  AS type_termination_type,
    valid_from_date                                                                   AS type_date,
    ROW_NUMBER() OVER ( PARTITION BY employee_id,type_date ORDER BY type_date DESC ) AS type_rank
  FROM {{ ref('bamboohr_employment_status_xf') }}
  WHERE employment_status = 'Terminated' 

),

term_reason AS (
  
  SELECT   
    *,
    ROW_NUMBER() OVER ( PARTITION BY employee_id,effective_date ORDER BY effective_date DESC ) AS term_reason_rank
  FROM {{ ref('workday_terminations') }} 

),

eda_sup AS (

  SELECT   
    uploaded_date,
    full_name,
    employee_id,
    ROW_NUMBER() over ( PARTITION BY full_name,uploaded_date ORDER BY uploaded_date DESC, employee_id DESC ) AS ldr_rank
  FROM blended_directory_source

),

dr AS (

  SELECT     
    uploaded_date,
    supervisor,
    COUNT(DISTINCT blend.employee_id) AS dr_total_direct_reports
  FROM blended_directory_source blend
  INNER JOIN eda_stage edi
  ON blend.employee_id = edi.employee_id
  AND blend.uploaded_date = edi.date_actual
  WHERE source_system = 'workday'
  GROUP BY 1, 2 

),

sup AS (

  SELECT    
    '2022-06-16'                    AS cutover_date,
    sup.uploaded_date               AS sup_date,
    sup.supervisor                  AS sup_name,
    eda_sup.employee_id             AS sup_id,
    sup.employee_id                 AS dr_id,
    sup.full_name                   AS dr_name,
    dr.dr_total_direct_reports
  FROM blended_directory_source sup
  LEFT JOIN eda_sup
    ON sup_name = eda_sup.full_name
    AND sup_date = eda_sup.uploaded_date
    AND 1 = ldr_rank
  LEFT JOIN dr
  ON dr_name = dr.supervisor
  AND sup_date = dr.uploaded_date
  WHERE sup_date >= cutover_date
  AND source_system = 'workday'
  ORDER BY 2 DESC, 3

),

dr_bhr AS (
  
  SELECT   
    date_actual                 AS sup_date_bhr,
    reports_to                  AS reports_to_bhr,
    reports_to_id               AS reports_to_id_bhr,
    COUNT(DISTINCT employee_id) AS total_direct_reports_bhr
  FROM eda_stage
  GROUP BY 1, 2, 3

),

staff_hist AS (
  
  SELECT  
    *,
    COALESCE(LAG(effective_date) OVER ( PARTITION BY employee_id ORDER BY effective_date DESC,date_time_initiated DESC ), '2099-01-01') AS next_effective_date
  FROM {{ ref('staffing_history_approved_source')}}
  
),

team_member_job_history AS (

  SELECT *
  FROM {{ ref('prep_team_member_job_history')}}

),

transfer AS (
  
  SELECT 
    DISTINCT employee_id,
    effective_date,
    MAX(business_process_reason)                                                               AS business_process_reason,
    MAX(IFF(COALESCE(job_code_past, '1') != COALESCE(job_code_current, '2'), 'TRUE', 'FALSE')) AS transfer_job_change
  FROM staff_hist
  WHERE business_process_category = 'Lateral Move'
  GROUP BY 1, 2
  
),

promo AS (
  
  SELECT *
  FROM {{ ref('bamboohr_promotions_xf') }}
  QUALIFY ROW_NUMBER() OVER ( PARTITION BY employee_id, promotion_date ORDER BY promotion_date DESC) = 1 
  
),

bonus AS (
  
  SELECT 
    employee_id,
    bonus_date,
    total_discretionary_bonuses
  FROM {{ ref('bamboohr_directionary_bonuses_xf') }} 

),

staff_hist_promo AS (
  

  SELECT 
    DISTINCT employee_id,
    effective_date,
    business_process_type
  FROM staff_hist
  WHERE business_process_type = 'Promote Employee Inbound' 
  
),

eda AS (
  
  SELECT    
    date_actual,
    eda_stage.employee_id,
    CASE
      WHEN date_actual >= sup.cutover_date THEN sup.sup_name
      ELSE reports_to
    END                                                                                                             AS reports_to,
    CASE
      WHEN date_actual >= sup.cutover_date THEN sup.sup_id
      ELSE reports_to_id
    END                                                                                                             AS reports_to_id,
    staff_hist.team_workday_id_current                                                                              AS team_id,
    full_name,
    CASE COALESCE(staff_hist.country_current, country)
      WHEN 'United States' THEN 'United States of America'
      ELSE COALESCE(staff_hist.country_current,country)
    END                                                                                                             AS country,
    CASE COALESCE(staff_hist.region_current, region_modified)
      WHEN 'NORAM' THEN 'Americas'
      WHEN 'JAPAC' THEN 'APJ'
      ELSE COALESCE(staff_hist.region_current, region_modified)
    END                                                                                                             AS region,
    location_factor,
    is_hire_date,
    is_termination_date,
    UPPER(IFF(staff_hist_promo.business_process_type = 'Promote Employee Inbound', 'TRUE', eda_stage.is_promotion)) AS is_promotion,
    IFF(LEFT(transfer.business_process_reason,8) = 'Transfer', 'TRUE', 'FALSE')                                     AS is_transfer,
    COALESCE(transfer.transfer_job_change, 'FALSE')                                                                 AS transfer_job_change,
    staff_hist.employee_type_current                                                                                AS employee_type,
    cost_center,
    team_member_job_history.job_title,
    team_member_job_history.business_title,
    team_member_job_history.job_specialty_single                                                                    AS jobtitle_speciality,
    team_member_job_history.job_profile_workday_id                                                                  AS job_id,
    team_member_job_history.job_family,
    team_member_job_history.job_role,
    team_member_job_history.job_grade,
    team_member_job_history.job_combination_id,
    CASE
      WHEN department IN ('TAM (inactive)',
      'TAM') THEN 'CSM'
      ELSE department
    END                                                                                                             AS department,
    CASE division
    WHEN 'Legal' THEN 'LACA'
      ELSE division
      END                                                                                                           AS division,
    CASE
      WHEN date_actual >= sup.cutover_date THEN COALESCE(sup.dr_total_direct_reports, 0)
      ELSE COALESCE(dr_bhr.total_direct_reports_bhr, 0)
    END                                                                                                             AS total_direct_reports,
    ''                                                                                                              AS termination_type,
    ''                                                                                                              AS termination_reason,
    ''                                                                                                              AS exit_impact,
    start_to_end.hire_date,
    start_to_end.hire_rank_asc                                                                                      AS hire_rank,
    start_to_end.term_date,
    start_to_end.term_rank_asc                                                                                      AS term_rank,
    start_to_end.last_date,
    bonus.total_discretionary_bonuses
  FROM eda_stage
  LEFT JOIN start_to_end
    ON eda_stage.employee_id = start_to_end.employee_id
      AND date_actual BETWEEN start_to_end.hire_date AND start_to_end.last_date
  LEFT JOIN sup
    ON eda_stage.employee_id = dr_id
      AND sup_date = date_actual
  LEFT JOIN dr_bhr
    ON eda_stage.employee_id = dr_bhr.reports_to_id_bhr
      AND date_actual = dr_bhr.sup_date_bhr
  LEFT JOIN staff_hist_promo
    ON eda_stage.employee_id = staff_hist_promo.employee_id
      AND eda_stage.date_actual = staff_hist_promo.effective_date
  LEFT JOIN transfer
    ON eda_stage.employee_id = transfer.employee_id
      AND eda_stage.date_actual = transfer.effective_date
  LEFT JOIN staff_hist
    ON eda_stage.employee_id = staff_hist.employee_id
      AND eda_stage.date_actual >= staff_hist.effective_date
      AND eda_stage.date_actual < staff_hist.next_effective_date
      AND '2022-06-16' <= eda_stage.date_actual
  LEFT JOIN bonus
    ON eda_stage.employee_id = bonus.employee_id
      AND eda_stage.date_actual = bonus.bonus_date
  LEFT JOIN team_member_job_history
    ON eda_stage.employee_id = team_member_job_history.employee_id
      AND eda_stage.date_actual >= team_member_job_history.valid_from
      AND eda_stage.date_actual <= team_member_job_history.valid_to
  WHERE date_actual <= CURRENT_DATE()

  UNION

  SELECT    
    date_actual + 1,
    eda_stage.employee_id,
    CASE
      WHEN date_actual >= sup.cutover_date THEN sup.sup_name
      ELSE reports_to
    END                                                                                                             AS reports_to,
    CASE
      WHEN date_actual >= sup.cutover_date THEN sup.sup_id
      ELSE reports_to_id
    END                                                                                                             AS reports_to_id,
    staff_hist.team_workday_id_current                                                                              AS team_id,
    full_name,
    CASE COALESCE(staff_hist.country_current, country)
      WHEN 'United States' THEN 'United States of America'
      ELSE COALESCE(staff_hist.country_current,country)
    END                                                                                                             AS country,
    CASE COALESCE(staff_hist.region_current, region_modified)
      WHEN 'NORAM' THEN 'Americas'
      WHEN 'JAPAC' THEN 'APJ'
      ELSE COALESCE(staff_hist.region_current, region_modified)
    END                                                                                                             AS region,
    location_factor,
    is_hire_date,
    is_termination_date,
    UPPER(IFF(staff_hist_promo.business_process_type = 'Promote Employee Inbound', 'TRUE', eda_stage.is_promotion)) AS is_promotion,
    IFF(LEFT(transfer.business_process_reason,8) = 'Transfer', 'TRUE', 'FALSE')                                     AS is_transfer,
    COALESCE(transfer.transfer_job_change, 'FALSE')                                                                 AS transfer_job_change,
    staff_hist.employee_type_current                                                                                AS employee_type,
    cost_center,
    team_member_job_history.job_title,
    team_member_job_history.business_title,
    team_member_job_history.job_specialty_single                                                                    AS jobtitle_speciality,
    team_member_job_history.job_profile_workday_id                                                                  AS job_id,
    team_member_job_history.job_family,
    team_member_job_history.job_role,
    team_member_job_history.job_grade,
    team_member_job_history.job_combination_id,
    CASE
      WHEN department IN ('TAM (inactive)',
      'TAM') THEN 'CSM'
      ELSE department
    END                                                                                                             AS department,
    CASE division
      WHEN 'Legal' THEN 'LACA'
      ELSE division
    END                                                                                                             AS division,
    CASE
      WHEN date_actual >= sup.cutover_date THEN COALESCE(sup.dr_total_direct_reports, 0)
      ELSE COALESCE(dr_bhr.total_direct_reports_bhr, 0)
    END                                                                                                             AS total_direct_reports,
    term_type.type_termination_type                                                                                 AS termination_type,
    term_reason.termination_reason,
    CASE term_reason.exit_impact
      WHEN 'Yes' THEN 'Regrettable'
      WHEN 'No' THEN 'Non-Regrettable'
    ELSE term_reason.exit_impact
    END                                                                                                             AS exit_impact,
    start_to_end.hire_date,
    start_to_end.hire_rank_asc                                                                                      AS hire_rank,
    start_to_end.term_date,
    start_to_end.term_rank_asc                                                                                      AS term_rank,
    start_to_end.last_date,
    bonus.total_discretionary_bonuses
  FROM eda_stage
  LEFT JOIN start_to_end
    ON eda_stage.employee_id = start_to_end.employee_id
      AND date_actual BETWEEN start_to_end.hire_date AND start_to_end.last_date
  LEFT JOIN term_type
    ON eda_stage.employee_id = type_id
      AND date_actual = type_date
      AND 1 = type_rank
  LEFT JOIN term_reason
    ON eda_stage.employee_id = term_reason.employee_id
      AND date_actual = effective_date
      AND 1 = term_reason_rank
  LEFT JOIN sup
    ON eda_stage.employee_id = dr_id
      AND sup_date = date_actual
  LEFT JOIN dr_bhr
    ON eda_stage.employee_id = dr_bhr.reports_to_id_bhr
      AND date_actual = dr_bhr.sup_date_bhr
  LEFT JOIN staff_hist_promo
    ON eda_stage.employee_id = staff_hist_promo.employee_id
      AND eda_stage.date_actual = staff_hist_promo.effective_date
  LEFT JOIN transfer
    ON eda_stage.employee_id = transfer.employee_id
      AND eda_stage.date_actual = transfer.effective_date
  LEFT JOIN staff_hist
    ON eda_stage.employee_id = staff_hist.employee_id
      AND eda_stage.date_actual >= staff_hist.effective_date
      AND eda_stage.date_actual < staff_hist.next_effective_date
      AND '2022-06-16' <= eda_stage.date_actual
  LEFT JOIN bonus
    ON eda_stage.employee_id = bonus.employee_id
      AND eda_stage.date_actual = bonus.bonus_date
  LEFT JOIN team_member_job_history
    ON eda_stage.employee_id = team_member_job_history.employee_id
      AND eda_stage.date_actual >= team_member_job_history.valid_from
      AND eda_stage.date_actual <= team_member_job_history.valid_to
  WHERE is_termination_date
    AND date_actual <= CURRENT_DATE() 

), 

pr AS (

  SELECT 
    employee_id                 AS pr_id,
    date_actual                 AS pr_date_actual,
    reports_to                  AS pr_reports_to,
    reports_to_id               AS pr_reports_to_id,
    team_id                     AS pr_team_id,
    country                     AS pr_country,
    region                      AS pr_region,
    location_factor             AS pr_location_factor,
    cost_center                 AS pr_cost_center,
    employee_type               AS pr_employee_type,
    job_title                   AS pr_job_title,
    jobtitle_speciality         AS pr_jobtitle_speciality,
    job_id                      AS pr_job_id,
    job_family                  AS pr_job_family,
    job_role                    AS pr_job_role,
    job_grade                   AS pr_job_grade,
    job_combination_id          AS pr_job_combination_id,
    department                  AS pr_department,
    division                    AS pr_division,
    total_direct_reports        AS pr_total_direct_reports,
    hire_rank                   AS pr_hire_rank
  FROM eda 

),

hist_stage AS (
  
  SELECT    
    employee_id AS cur_id,
    full_name,
    date_actual AS cur_date_actual,
    CASE
      WHEN date_actual = term_date + 1 THEN 'T'
      ELSE 'A'
    END AS employment_status,
    CASE
      WHEN employment_status = 'T' THEN cur_date_actual
      ELSE COALESCE(LAG(cur_date_actual) OVER ( PARTITION BY employee_id, hire_rank ORDER BY cur_date_actual DESC, hire_rank DESC ) - 1, CURRENT_DATE())
    END                  AS end_date,
    reports_to           AS cur_reports_to,
    reports_to_id        AS cur_reports_to_id,
    team_id              AS cur_team_id,
    country              AS cur_country,
    regiON               AS cur_region,
    location_factor      AS cur_location_factor,
    cost_center          AS cur_cost_center,
    employee_type        AS cur_employee_type,
    is_promotion         AS cur_is_promotion,
    is_transfer          AS cur_is_transfer,
    transfer_job_change  AS cur_transfer_job_change,
    job_role             AS cur_job_role,
    job_grade            AS cur_job_grade,
    job_title            AS cur_job_title,
    jobtitle_speciality  AS cur_jobtitle_speciality,
    job_id               AS cur_job_id,
    job_family           AS cur_job_family,
    job_combination_id   AS cur_job_combination_id,
    department           AS cur_department,
    division             AS cur_division,
    total_direct_reports AS cur_total_direct_reports,
    hire_date,
    hire_rank,
    last_date,
    term_date,
    term_rank,
    termination_type,
    termination_reason,
    exit_impact,
    total_discretionary_bonuses,
    pr.*,
    CASE
      WHEN hire_date = cur_date_actual THEN 1
      WHEN term_date + 1 = date_actual THEN 1
      WHEN COALESCE(cur_reports_to, '1') != COALESCE(pr_reports_to, '1') THEN 1
      WHEN COALESCE(cur_reports_to_id, '1') != COALESCE(pr_reports_to_id, '1') THEN 1
      WHEN COALESCE(cur_team_id, '1') != COALESCE(pr_team_id, '1') THEN 1
      WHEN COALESCE(cur_country, '1') != COALESCE(pr_country, '1') THEN 1
      WHEN COALESCE(cur_region, '1') != COALESCE(pr_region, '1') THEN 1
      WHEN COALESCE(cur_cost_center, '1') != COALESCE(pr_cost_center, '1') THEN 1
      WHEN COALESCE(cur_job_title, '1') != COALESCE(pr_job_title, '1') THEN 1
      WHEN COALESCE(cur_department, '1') != COALESCE(pr_department, '1') THEN 1
      WHEN COALESCE(cur_division, '1') != COALESCE(pr_division, '1') THEN 1
      WHEN COALESCE(cur_total_direct_reports, 0) != COALESCE(pr_total_direct_reports, 0) THEN 1
      WHEN COALESCE(cur_job_role, '1') != COALESCE(pr_job_role, '1') THEN 1
      WHEN COALESCE(cur_jobtitle_speciality, '1') != COALESCE(pr_jobtitle_speciality, '1') THEN 1
      WHEN COALESCE(cur_job_id,'1') != COALESCE(pr_job_id,'1') THEN 1
      WHEN COALESCE(cur_job_family,'1') != COALESCE(pr_job_family,'1') THEN 1
      WHEN COALESCE(cur_location_factor, '1') != COALESCE(pr_location_factor, '1') THEN 1
      WHEN COALESCE(cur_job_grade, '1') != COALESCE(pr_job_grade, '1') THEN 1
      WHEN COALESCE(cur_employee_type, '1') != COALESCE(pr_employee_type, '1') THEN 1
      WHEN cur_is_promotion = 'TRUE' THEN 1
      WHEN cur_is_transfer = 'TRUE' AND cur_transfer_job_change = 'TRUE' THEN 1
      WHEN total_discretionary_bonuses >= 1 THEN 1
      ELSE 0
    END AS filter
  FROM eda cur
  LEFT JOIN pr
    ON cur_id = pr_id
      AND cur_date_actual - 1 = pr_date_actual
      AND hire_rank = pr_hire_rank
  WHERE filter = 1
    AND cur_date_actual <= CURRENT_DATE()
  ORDER BY 1, 2 DESC 

),

job_history AS (

  SELECT   
    employee_id,
    hire_rank,
    date_actual,
    job_title,
    job_role,
    COALESCE(LEAD(job_title) OVER ( PARTITION BY employee_id, hire_rank ORDER BY date_actual DESC ), job_title) AS pr_job_title,
    is_hire_date,
    is_termination_date
  FROM eda 
  QUALIFY pr_job_title <> job_title OR is_hire_date = 'TRUE' 

),

job_date_stage AS (
  
  SELECT   
    employee_id,
    hire_rank,
    date_actual                                                                                                           AS job_start_date,
    COALESCE(lag(date_actual) OVER ( PARTITION BY employee_id, hire_rank ORDER BY date_actual DESC ) - 1, CURRENT_DATE()) AS job_end_date,
    job_title,
    job_role
  FROM job_history
  ORDER BY 1,2 DESC

),

job_date AS (

  SELECT
    job_date_stage.employee_id,
    job_date_stage.hire_rank,
    job_date_stage.job_start_date,
    job_date_stage.job_end_date,
    COALESCE(job_date_stage.job_title,eda.job_title) AS job_title, --original job title or day prior if null
    COALESCE(eda.job_role,job_date_stage.job_role)   AS job_role   --job_role from day prior to title change
  FROM job_date_stage
  LEFT JOIN eda on job_date_stage.employee_id = eda.employee_id
    AND job_date_stage.hire_rank = eda.hire_rank
    AND job_date_stage.job_end_date = eda.date_actual

)

SELECT   
  cur_id AS employee_id,
  hist_stage.full_name,
  hist_stage.employment_status,
  cur_date_actual AS min_date,
  end_date        AS max_date,
  CASE
    WHEN hire_date = min_date AND hist_stage.hire_rank = 1 THEN 'Hire'
    WHEN hire_date = min_date AND hist_stage.hire_rank > 1 THEN 'Rehire'
    WHEN hist_stage.employment_status = 'T' THEN 'Termination'
    WHEN cur_is_promotion = 'TRUE' OR min_date = promotion_date::DATE THEN 'Promotion'
    WHEN cur_is_transfer = 'TRUE' AND cur_transfer_job_change = 'TRUE' THEN 'Transfer'
    WHEN COALESCE(hist_stage.cur_employee_type, '1') != COALESCE(pr_employee_type, '1') THEN 'Employee Type Change'
    WHEN COALESCE(hist_stage.cur_region, '1') != COALESCE(pr_region, '1') THEN 'Organization Change'
    WHEN COALESCE(hist_stage.cur_country, '1') != COALESCE(pr_country, '1') THEN 'Organization Change'
    WHEN COALESCE(hist_stage.cur_cost_center, '1') != COALESCE(pr_cost_center, '1') THEN 'Organization Change'
    WHEN COALESCE(hist_stage.cur_division, '1') != COALESCE(pr_division, '1') THEN 'Organization Change'
    WHEN COALESCE(hist_stage.cur_department, '1') != COALESCE(pr_department, '1') THEN 'Organization Change'
    WHEN COALESCE(hist_stage.cur_job_title, '1') != COALESCE(pr_job_title, '1') THEN 'Job Title Change'
    WHEN COALESCE(hist_stage.cur_reports_to, '1') != COALESCE(pr_reports_to, '1') THEN 'Supervisor Change'
    WHEN COALESCE(hist_stage.cur_reports_to_id, '1') != COALESCE(pr_reports_to_id, '1') THEN 'Supervisor Change'
    WHEN COALESCE(hist_stage.cur_team_id, '1') != COALESCE(pr_team_id, '1') THEN 'Supervisor Org Change'
    WHEN COALESCE(hist_stage.cur_total_direct_reports, '1') != COALESCE(pr_total_direct_reports, '1') THEN 'Span of Control Change'
    WHEN COALESCE(hist_stage.cur_job_role, '1') != COALESCE(pr_job_role, '1') THEN 'Job Role Change'
    WHEN COALESCE(hist_stage.cur_jobtitle_speciality, '1') != COALESCE(pr_jobtitle_speciality, '1') THEN 'Job Speciality Change'
    WHEN COALESCE(hist_stage.cur_location_factor, '1') != COALESCE(pr_location_factor, '1') THEN 'Location Factor Change'
    WHEN COALESCE(hist_stage.cur_job_grade, '1') != COALESCE(pr_job_grade, '1') THEN 'Job Grade Change'
    WHEN COALESCE(hist_stage.cur_job_id,'1') != COALESCE(pr_job_id,'1') THEN 'Job Title Change'
    WHEN COALESCE(hist_stage.cur_job_family,'1') != COALESCE(pr_job_family,'1') THEN 'Job Family Change'
    WHEN hist_stage.total_discretionary_bonuses >= 1 THEN 'Discretionary Bonus'
    ELSE NULL
  END                                AS job_change_reason,
  hist_stage.cur_country             AS country,
  hist_stage.cur_region              AS region,
  hist_stage.cur_location_factor     AS location_factor,
  hist_stage.cur_cost_center         AS cost_center,
  hist_stage.cur_division            AS division,
  hist_stage.cur_department          AS department,
  hist_stage.cur_job_role            AS job_role,
  hist_stage.cur_job_grade           AS job_grade,
  hist_stage.cur_job_title           AS job_title,
  hist_stage.cur_jobtitle_speciality AS jobtitle_speciality,
  hist_stage.cur_job_id              AS job_id,
  hist_stage.cur_job_family          AS job_family,  
  hist_stage.cur_employee_type       AS employee_type,
  cur_reports_to                     AS reports_to,
  cur_reports_to_id                  AS reports_to_id,
  cur_team_id                        AS team_id,
  cur_total_direct_reports           AS total_direct_reports,
  hire_date,
  hist_stage.hire_rank,
  last_date,
  term_date + 1                      AS termination_date,
  term_rank,
  termination_type,
  termination_reason,
  exit_impact,
  job_date.job_start_date,
  LEAST(last_date,job_date.job_end_date) AS job_end_date,
  COALESCE(total_discretionary_bonuses,0) AS discretionary_bonus_count,
  pr_job.job_role                         AS prior_job_role,
  pr_job.job_title                        AS prior_job_title,
  pr_job.job_start_date                   AS prior_job_start_date,
  pr_job.job_end_date                     AS prior_job_end_date
FROM  hist_stage
LEFT JOIN promo
  ON hist_stage.cur_id = promo.employee_number
    AND hist_stage.cur_date_actual = promo.promotion_date::DATE
LEFT JOIN job_date
  ON hist_stage.cur_id = job_date.employee_id
    AND hist_stage.hire_rank = job_date.hire_rank
    AND IFF(employment_status = 'T', hist_stage.last_date, hist_stage.cur_date_actual)
    BETWEEN job_date.job_start_date AND job_date.job_end_date
LEFT JOIN job_date AS pr_job
  ON hist_stage.cur_id = pr_job.employee_id
    AND hist_stage.hire_rank = pr_job.hire_rank
    AND DATEADD('d', -1, job_date.job_start_date) BETWEEN pr_job.job_start_date AND pr_job.job_end_date
    AND pr_job.job_start_date BETWEEN hire_date AND last_date
WHERE NOT COALESCE(hist_stage.cur_employee_type,'') IN ('Intern (Trainee)')
ORDER BY  hist_stage.cur_id ASC, min_date DESC