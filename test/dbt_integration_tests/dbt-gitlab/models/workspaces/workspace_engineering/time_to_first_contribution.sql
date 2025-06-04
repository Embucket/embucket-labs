WITH gitlab_dotcom_users AS (

    SELECT
        LOWER(user_name) AS user_name,
        user_id,
        ROW_NUMBER() OVER (
        PARTITION BY LOWER(user_name) ORDER BY COALESCE(dbt_valid_to, CURRENT_DATE) DESC
        ) AS a
    FROM {{ ref('gitlab_dotcom_users_snapshots_source') }}
    QUALIFY a = 1
    
), employees AS (

    SELECT 
        employee_id, 
        user_id, 
        hire_date,
        department,
        division
    FROM {{ ref('employee_directory_analysis') }}
    LEFT JOIN gitlab_dotcom_users ON gitlab_dotcom_users.user_name = employee_directory_analysis.gitlab_username
    WHERE employee_directory_analysis.division = 'Engineering'
    GROUP BY 1,2,3,4,5
    HAVING hire_date >= dateadd('month',-24,current_date)
    
), mrs AS (

    SELECT DISTINCT 
        emp.*,
        mrs.*
    FROM employees emp
    LEFT JOIN {{ ref('engineering_merge_requests') }} mrs ON emp.user_id = mrs.author_id
    WHERE mrs.merged_at IS NOT null
    
), first_mr AS (

    SELECT 
        employee_id, 
        user_id, 
        hire_date, 
        department,
        division,
        date_trunc('month',hire_date) AS start_month, 
        merge_request_id, 
        created_at::date AS created_at, 
        url, 
        datediff('day',hire_date,created_at) AS time_to_first_mr,
        ROW_NUMBER() OVER (
        PARTITION BY user_id, hire_date ORDER BY created_at
        ) AS a
    FROM mrs
    WHERE created_at >= hire_date
    QUALIFY a = 1

)

SELECT
    start_month, 
    department,
    division,
    COUNT(DISTINCT employee_id) AS new_hires,
    avg(time_to_first_mr) AS avg_time_to_first_mr, 
    median(time_to_first_mr) AS median_time_to_first_mr
FROM first_mr
GROUP BY 1,2,3
ORDER BY 1 DESC