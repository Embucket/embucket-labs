WITH engineering_merge_requests AS (

    SELECT *
    FROM {{ ref('engineering_merge_requests') }}

), dim_note AS (

    SELECT *
    FROM {{ ref('dim_note') }}

), users AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_users_snapshots_source') }}

), stages_groups_yaml_source AS (

    SELECT *
    FROM {{ ref('stages_groups_yaml_source') }}

), employee_directory_analysis AS (

    SELECT *
    FROM {{ ref('employee_directory_analysis') }}

), gitlab_dotcom_users AS (

    SELECT
        LOWER(user_name) AS user_name,
        user_id,
        user_type,
        ROW_NUMBER() OVER (
        PARTITION BY LOWER(user_name) ORDER BY COALESCE(dbt_valid_to, CURRENT_DATE) DESC
        ) AS a
    FROM users
    WHERE user_type = 0
    AND LOWER(user_name) NOT LIKE '%gitlab%bot%'
    AND LOWER(user_name) NOT LIKE '%project%bot%'
    AND LOWER(user_name) NOT LIKE '%group%bot%'
    AND LOWER(user_name) NOT LIKE '%gl-service%'
    AND LOWER(user_name) NOT IN('gitalybot','kubitus-bot','gl-support-bot','gl-service-appsec-depsaster','ops-gitlab-net','engineering-bot')
    QUALIFY a = 1

), product_categories_yml_base AS (

  SELECT DISTINCT
    LOWER(group_name)                                                         AS group_name,
    REPLACE(LOWER(stage_section) ,'_',' ')                                    AS section_name,
    LOWER(stage_display_name)                                                 AS stage_name,
    IFF(group_name LIKE '%::%', SPLIT_PART(LOWER(group_name), '::', 1), NULL) AS root_name
  FROM stages_groups_yaml_source
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM stages_groups_yaml_source)

), product_categories_yml AS (

  SELECT
    group_name,
    section_name,
    stage_name
  FROM product_categories_yml_base
  UNION ALL
  SELECT DISTINCT
    root_name AS group_name,
    section_name,
    stage_name
  FROM product_categories_yml_base
  WHERE root_name IS NOT NULL
  UNION ALL
  SELECT
    'technical writer' AS group_name,
    '' AS section_name,
    '' AS stage_name

), employees_metadata AS (

    SELECT 
        employee_id, 
        LOWER(gitlab_username) AS gitlab_username,
        CASE WHEN lower(job_title) LIKE '%technical%writer%' THEN 'technical writer' ELSE jobtitle_speciality END AS jobtitle_speciality_modified,
        TRIM(SUBSTRING(LOWER(TRIM(VALUE::VARCHAR)),charindex(':',LOWER(TRIM(VALUE::VARCHAR)))+1,100))   AS job_title_speciality_cleansed,
    FROM employee_directory_analysis,
    LATERAL FLATTEN(INPUT=>SPLIT(COALESCE(REPLACE(jobtitle_speciality_modified,'&',','),''), ','))
    JOIN product_categories_yml ON job_title_speciality_cleansed = product_categories_yml.group_name
    WHERE date_actual = current_date
    GROUP BY 1,2,3,4

), employees_final AS (

    SELECT
        employee_id
    FROM employees_metadata
    GROUP BY 1
    HAVING COUNT(DISTINCT job_title_speciality_cleansed) = 1

), employees AS (

    SELECT 
        employees_metadata.*,
        user_id,
        user_name,  
    FROM employees_metadata
    JOIN employees_final ON employees_metadata.employee_id = employees_final.employee_id
    JOIN gitlab_dotcom_users ON gitlab_dotcom_users.user_name = employees_metadata.gitlab_username
    
), notes AS (

    SELECT DISTINCT
        notes.author_id, 
        notes.created_at::date AS note_created_at,
        gitlab_dotcom_users.user_name,
        engineering_merge_requests.*,
        gitlab_dotcom_users.user_type,
        employees.job_title_speciality_cleansed
    FROM dim_note notes
    JOIN engineering_merge_requests ON notes.noteable_id = engineering_merge_requests.merge_request_id AND noteable_type = 'MergeRequest'
    JOIN gitlab_dotcom_users ON notes.author_id = gitlab_dotcom_users.user_id
    LEFT JOIN employees ON notes.author_id = employees.user_id
    WHERE engineering_merge_requests.merged_at IS NOT NULL
    AND job_title_speciality_cleansed IS NOT NULL
    AND DATE_TRUNC('month',engineering_merge_requests.merged_at) >= DATEADD('month',-24,current_date)
    AND (notes.action_type IS NULL
    OR notes.action_type IN (
    'merge',
    'merged',
    'approved',
    'commit',
    'closed',
    'description',
    'discussion',
    'opened',
    'assignee',
    'reviewer',
    'task',
    'branch',
    'cherry_pick',
    'outdated',
    'requested_changes',
    'unapproved'
    ))    

), mrs AS (

    SELECT 
        merge_request_id,
        created_at,
        created_month,
        merged_at,
        merge_month,
        url,
        ARRAY_AGG(DISTINCT job_title_speciality_cleansed) AS jobtitle_speciality
    FROM notes
    GROUP BY 1,2,3,4,5,6
    
), final AS (

    SELECT
        *
        , ARRAY_SIZE(jobtitle_speciality) AS count_groups
    FROM mrs
)

SELECT merge_month
, MEDIAN(count_groups) as median_count_groups
, AVG(count_groups) AS avg_count_groups
, MIN(count_groups) AS min_count_groups
, MAX(count_groups) AS max_count_groups
FROM final
GROUP BY 1
ORDER BY 1 DESC