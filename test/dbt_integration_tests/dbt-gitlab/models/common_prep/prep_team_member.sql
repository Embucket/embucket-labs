WITH all_team_members AS (

  SELECT
    *,
    preferred_first_name || ' ' || preferred_last_name AS full_name
  FROM {{ ref('all_workers_source') }}

),

key_talent AS (

  SELECT
    employee_id,
    key_talent,
    effective_date                                                                                           AS valid_from,
    LEAD(valid_from, 1, {{ var('tomorrow') }}) OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to
  FROM {{ ref('assess_talent_source') }}

),

gitlab_usernames AS (

  SELECT
    employee_id,
    gitlab_username,
    effective_date                                                                                                                AS valid_from,
    LEAD(valid_from, 1, {{ var('tomorrow') }}) OVER (PARTITION BY employee_id ORDER BY valid_from, date_time_completed) AS valid_to
  FROM {{ ref('gitlab_usernames_source') }}

),

performance_growth_potential AS (

  SELECT
    employee_id,
    growth_potential_rating,
    performance_rating,
    review_period_end_date                                                                                   AS valid_from,
    LEAD(valid_from, 1, {{ var('tomorrow') }}) OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to
  FROM {{ ref('performance_growth_potential_source') }}

),

team_member_info AS (

  /*
  We need to identify and isolate groups of consecutive records that share the same country, region, employee_type, or team (islands)
  and the gaps between those groups
  */

  SELECT
    {{ dbt_utils.generate_surrogate_key(['employee_id', 'team_id_current', 'country_current','region_current','employee_type_current']) }} AS unique_key,
    employee_id                                                                                                                            AS employee_id,
    team_id_current                                                                                                                        AS team_id,
    country_current                                                                                                                        AS country,
    region_current                                                                                                                         AS region,
    employee_type_current                                                                                                                  AS employee_type,
    effective_date                                                                                                                         AS valid_from,
    LEAD(valid_from, 1, {{ var('tomorrow') }}) OVER (PARTITION BY employee_id ORDER BY valid_from)                                         AS valid_to
  FROM {{ ref('staffing_history_approved_source') }}

),

staffing_history AS (

  /*
  This CTE pulls the remaining fields we need from staffing_history_approved_source
  */

  SELECT
    employee_id,
    business_process_type,
    hire_date,
    IFF(business_process_type IN (
        'End Contingent Worker Contract',
        'Termination'
      ), effective_date, NULL)                                                                                                  AS termination_date,
    LAST_VALUE(hire_date IGNORE NULLS) OVER (PARTITION BY employee_id ORDER BY effective_date ROWS UNBOUNDED PRECEDING)         AS most_recent_hire_date,
    IFF(business_process_type IN (
			  'End Contingent Worker Contract',
        'Termination'
			), FALSE, TRUE)                                                                                                           AS is_current_team_member,
    IFF(COUNT(hire_date) OVER (PARTITION BY employee_id ORDER BY effective_date ASC ROWS UNBOUNDED PRECEDING) > 1, TRUE, FALSE) AS is_rehire, -- team member is a rehire if they have more than 1 hire_date event
    effective_date                                                                                                              AS valid_from,
    LEAD(valid_from, 1, {{ var('tomorrow') }}) OVER (PARTITION BY employee_id ORDER BY valid_from)                              AS valid_to
  FROM {{ ref('staffing_history_approved_source') }}
  WHERE business_process_type in ('Hire', 'Termination', 'Contract Contingent Worker', 'End Contingent Worker Contract')

),

history_combined AS (

  /*
  This CTE combines the fields from staffing history and the fields we want to keep track of (country, region, team_id) from
  the team_member_info CTE
  */

  SELECT
    staffing_history.employee_id                                       AS employee_id,
    staffing_history.hire_date                                         AS hire_date,
    staffing_history.termination_date                                  AS termination_date,
    staffing_history.most_recent_hire_date                             AS most_recent_hire_date,
    /*team_id didn't exist before Workday. To avoid confusion, nullify the value before the Workday
    cutover date
    */
    CASE WHEN team_member_info.valid_from >= '2022-06-16'
        THEN team_member_info.team_id
    END                                                                AS team_id,
    team_member_info.country                                           AS country,
    team_member_info.region                                            AS region,
    team_member_info.employee_type                                     AS employee_type,
    staffing_history.is_current_team_member                            AS is_current_team_member,
    staffing_history.is_rehire                                         AS is_rehire,
    GREATEST(team_member_info.valid_from, staffing_history.valid_from) AS valid_from,
    LEAST(team_member_info.valid_to, staffing_history.valid_to)        AS valid_to
  FROM staffing_history
  LEFT JOIN team_member_info
    ON staffing_history.employee_id = team_member_info.employee_id
      AND NOT (
        staffing_history.valid_from >= team_member_info.valid_to
        OR staffing_history.valid_to <= team_member_info.valid_from
      )

),

unioned AS (

  /*
    We union all valid_from dates from each type 2 SCD table (except the type 1 SCD - all_team_members)
    to create a date spine that we can then use to join our events into
  */

  SELECT
    employee_id,
    valid_from AS unioned_dates
  FROM key_talent

  UNION

  SELECT
    employee_id,
    valid_from
  FROM gitlab_usernames

  UNION

  SELECT
    employee_id,
    valid_from
  FROM performance_growth_potential

  UNION

  SELECT
    employee_id,
    valid_from
  FROM history_combined

),

date_range AS (

  SELECT
    employee_id,
    unioned_dates                                                                                            AS valid_from,
    LEAD(valid_from, 1, {{ var('tomorrow') }}) OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to,
    IFF(valid_to = {{ var('tomorrow') }}, TRUE, FALSE)                                             AS is_current
  FROM unioned

),

final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['all_team_members.employee_id']) }}                                                                                        AS dim_team_member_sk,
    all_team_members.employee_id                                                                                                                                                                     AS employee_id,
    COALESCE(all_team_members.nationality, 'Unknown Nationality')                                                                                                                                    AS nationality,
    COALESCE(all_team_members.ethnicity, 'Unknown Ethnicity')                                                                                                                                        AS ethnicity,
    COALESCE(all_team_members.preferred_first_name, 'Unknown First Name')                                                                                                                            AS first_name,
    COALESCE(all_team_members.preferred_last_name, 'Unknown Last Name')                                                                                                                              AS last_name,
    COALESCE(all_team_members.full_name, 'Unknown Full Name')                                                                                                                                        AS full_name,
    COALESCE(all_team_members.gender, 'Unknown Gender')                                                                                                                                              AS gender,
    COALESCE(all_team_members.work_email, 'Unknown Work Email')                                                                                                                                      AS work_email,
    all_team_members.date_of_birth                                                                                                                                                                   AS date_of_birth,
    COALESCE(key_talent.key_talent, 'Unknown Yes/No Status')                                                                                                                                         AS key_talent_status,
    DATEDIFF(YEAR, date_of_birth, date_range.valid_from) + CASE WHEN (DATEADD(YEAR, DATEDIFF(YEAR, date_of_birth, date_range.valid_from), date_of_birth) > date_range.valid_from) THEN -1 ELSE 0 END
      AS age_calculated,
    CASE
      WHEN age_calculated BETWEEN 18 AND 24 THEN '18-24'
      WHEN age_calculated BETWEEN 25 AND 29 THEN '25-29'
      WHEN age_calculated BETWEEN 30 AND 34 THEN '30-34'
      WHEN age_calculated BETWEEN 35 AND 39 THEN '35-39'
      WHEN age_calculated BETWEEN 40 AND 44 THEN '40-44'
      WHEN age_calculated BETWEEN 44 AND 49 THEN '44-49'
      WHEN age_calculated BETWEEN 50 AND 54 THEN '50-54'
      WHEN age_calculated BETWEEN 55 AND 59 THEN '55-59'
      WHEN age_calculated >= 60 THEN '60+'
      WHEN age_calculated IS NULL THEN 'Unreported'
      WHEN age_calculated = -1 THEN 'Unreported'
    END                                                                                                                                                                                              AS age_cohort,
    CASE
      WHEN region = 'Americas' AND history_combined.country IN ('United States', 'Canada', 'Mexico', 'United States of America')
        THEN 'NORAM'
      WHEN region = 'Americas' AND history_combined.country NOT IN ('United States', 'Canada', 'Mexico', 'United States of America')
        THEN 'LATAM'
      ELSE region
    END                                                                                                                                                                                              AS region_modified,
    IFF(
      history_combined.country IN ('United States', 'United States of America'),
      COALESCE(gender, 'Did Not Identify') || '_' || 'United States of America',
      COALESCE(gender, 'Did Not Identify') || '_' || 'Non-US'
    )                                                                                                                                                                                                AS gender_region,
    IFF(
      history_combined.country IN ('United States', 'United States of America'),
      COALESCE(ethnicity, 'Did Not Identify') || '_' || 'United States of America',
      COALESCE(ethnicity, 'Did Not Identify') || '_' || 'Non-US'
    )                                                                                                                                                                                                AS ethnicity_region,
    COALESCE (COALESCE(ethnicity, 'Did Not Identify') NOT IN ('White', 'Asian', 'Did Not Identify', 'Declined to Answer'), FALSE)                                                                    AS urg_group,
    IFF(urg_group = TRUE, 'URG', 'Non-URG') || '_'
    || IFF(
      history_combined.country IN ('United States', 'United States of America'),
      'United States of America',
      'Non-US'
    )                                                                                                                                                                                                AS urg_region,
    COALESCE(gitlab_usernames.gitlab_username, 'Unknown Username')                                                                                                                                   AS gitlab_username,
    COALESCE(performance_growth_potential.growth_potential_rating, 'Unknown Rating')                                                                                                                 AS growth_potential_rating,
    COALESCE(performance_growth_potential.performance_rating, 'Unknown Rating')                                                                                                                      AS performance_rating,
    COALESCE(history_combined.country, 'Unknown Country')                                                                                                                                            AS country,
    COALESCE(history_combined.region, 'Unknown Region')                                                                                                                                              AS region,
    COALESCE(history_combined.team_id, 'Unknown Team ID')                                                                                                                                            AS team_id,
    COALESCE(history_combined.employee_type, 'Unknown Employee Type')                                                                                                                                AS employee_type,
    history_combined.most_recent_hire_date                                                                                                                                                           AS hire_date,
    history_combined.termination_date                                                                                                                                                                AS termination_date,
    history_combined.is_current_team_member                                                                                                                                                          AS is_current_team_member,
    history_combined.is_rehire                                                                                                                                                                       AS is_rehire,
    date_range.valid_from                                                                                                                                                                            AS valid_from,
    date_range.valid_to                                                                                                                                                                              AS valid_to,
    date_range.is_current                                                                                                                                                                            AS is_current
  FROM all_team_members
  INNER JOIN date_range
    ON all_team_members.employee_id = date_range.employee_id

    /*
      A team member event is matched to a date interval if there is any overlap between the two,
      this happens when the team member event begins before the date range ends
      and the team member event ends after the range begins
    */

  LEFT JOIN key_talent
    ON date_range.employee_id = key_talent.employee_id
      AND NOT (
        date_range.valid_from >= key_talent.valid_to
        OR date_range.valid_to <= key_talent.valid_from
      )
  LEFT JOIN gitlab_usernames
    ON date_range.employee_id = gitlab_usernames.employee_id
      AND NOT (
        date_range.valid_from >= gitlab_usernames.valid_to
        OR date_range.valid_to <= gitlab_usernames.valid_from
      )
      AND gitlab_usernames.valid_from != gitlab_usernames.valid_to
  LEFT JOIN performance_growth_potential
    ON date_range.employee_id = performance_growth_potential.employee_id
      AND NOT (
        date_range.valid_from >= performance_growth_potential.valid_to
        OR date_range.valid_to <= performance_growth_potential.valid_from
      )
  LEFT JOIN history_combined
    ON date_range.employee_id = history_combined.employee_id
      AND NOT (
        date_range.valid_from >= history_combined.valid_to
        OR date_range.valid_to <= history_combined.valid_from
      )

)

SELECT *
FROM final
