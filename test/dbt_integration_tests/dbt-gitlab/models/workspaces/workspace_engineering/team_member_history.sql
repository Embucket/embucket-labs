WITH dates AS (

  SELECT date_actual
  FROM {{ ref('dim_date') }}
  WHERE date_actual >= '2020-12-01'

),

category AS (

  SELECT
    SPLIT_PART(LOWER(group_name), ':', 1)     AS group_name,
    LOWER(stage_display_name)                 AS stage_display_name,
    stage_section,
    MIN(snapshot_date)                        AS valid_from,
    MAX(snapshot_date)                        AS valid_to,
    ROW_NUMBER() OVER (
      PARTITION BY SPLIT_PART(LOWER(group_name), ':', 1)
      ORDER BY valid_to
    )                                         AS rn,
    IFF(valid_to = CURRENT_DATE, TRUE, FALSE) AS is_current
  FROM {{ ref('stages_groups_yaml_historical') }}
  {{ dbt_utils.group_by(n=3) }}

),

gitlab_dotcom_users AS (

  SELECT
    LOWER(user_name) AS user_name,
    user_id,
    ROW_NUMBER() OVER (
      PARTITION BY LOWER(user_name)
      ORDER BY COALESCE(dbt_valid_to, CURRENT_DATE) DESC
    )                AS a
  FROM {{ ref('gitlab_dotcom_users_snapshots_source') }}
  QUALIFY a = 1

),

member_source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_members') }} AS members_source
  WHERE is_currently_valid
    AND member_source_type = 'Namespace'
    AND {{ filter_out_blocked_users('members_source', 'user_id') }}

),

team_yml AS (

  SELECT
    gitlab_username,
    departments                       AS teams,
    ARRAY_TO_STRING(departments, '|') AS teams_list,
    snapshot_date
  FROM {{ ref('team_yaml_historical') }}
  WHERE gitlab_username IS NOT NULL AND gitlab_username != 'TBD'

),

directory_mapping AS (

  SELECT
    a.date_actual,
    REPLACE(b.gitlab_username, '@', '')                                                                                                                                 AS gitlab_username,
    d.user_id,
    b.employee_id,
    CONCAT(COALESCE(job_specialty_multi, ''), '; ', COALESCE(job_specialty_single, ''))                                                                                 AS job_specialty,
    --sometimes specialty is only specified in either single/multi, this way we can extract the group name properly

    b.division,
    CASE WHEN b.department IN ('Core Development', 'Expansion', 'Development') AND a.date_actual < '2023-11-01' THEN 'Development'
      ELSE b.department
    END                              AS department, --to keep Development historic data before the split (split date is chosen from https://gitlab.com/gitlab-org/quality/cloud-finops/team-tasks/-/issues/346#note_1646832441)
    b.position,
    b.management_level,
    CASE
      WHEN LOWER(b.position) LIKE '%backend%'
        THEN 'backend'
      WHEN LOWER(b.position) LIKE '%fullstack%'
        THEN 'fullstack'
      WHEN LOWER(b.position) LIKE '%frontend%'
        THEN 'frontend'
    END                                                                                                                                                                 AS technology_group,
    f.teams_list,
    MAX(IFF(CONTAINS(LOWER(job_specialty), c.group_name), c.group_name, NULL))                                                                                          AS user_group,
    MAX(IFF(CONTAINS(LOWER(job_specialty), c.group_name), c.stage_display_name, IFF(CONTAINS(LOWER(job_specialty), c.stage_display_name), c.stage_display_name, NULL))) AS user_stage,
    MAX(IFF(CONTAINS(LOWER(job_specialty), c.group_name), c.stage_section, IFF(CONTAINS(LOWER(job_specialty), c.stage_display_name), c.stage_section, NULL)))           AS user_section,
    ARRAY_AGG(DISTINCT e.source_id) WITHIN GROUP (ORDER BY e.source_id)                                                                                                 AS is_member_in_namespace,
    IFF(date_actual = MAX(date_actual) OVER (PARTITION BY 1), TRUE, FALSE)                                                                                              AS is_most_recent
  FROM dates AS a
  INNER JOIN {{ ref('mart_team_member_directory') }} AS b
    ON b.is_current_team_member
      AND a.date_actual >= b.valid_from
      AND a.date_actual < b.valid_to
  INNER JOIN category AS c ON a.date_actual BETWEEN c.valid_from AND c.valid_to
  LEFT JOIN gitlab_dotcom_users AS d ON REPLACE(LOWER(b.gitlab_username), '@', '') = LOWER(d.user_name)
  LEFT JOIN member_source AS e ON d.user_id = e.user_id AND a.date_actual >= DATE_TRUNC('day', e.invite_created_at)
  LEFT JOIN team_yml AS f ON a.date_actual = f.snapshot_date AND REPLACE(LOWER(b.gitlab_username), '@', '') = LOWER(f.gitlab_username)
  {{ dbt_utils.group_by(n=11) }}

)

SELECT *
FROM directory_mapping
