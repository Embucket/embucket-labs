-- depends_on: {{ ref('engineering_productivity_metrics_projects_to_include') }}
-- depends_on: {{ ref('projects_part_of_product') }}

{{ simple_cte([
    ('gitlab_dotcom_merge_requests', 'prep_merge_request'),
    ('gitlab_ops_merge_requests', 'gitlab_ops_merge_requests_xf'),
    ('mapped_employee', 'map_team_member_bamboo_gitlab_dotcom_gitlab_ops'),
    ('employee_directory', 'employee_directory_analysis'),
    ('prep_project', 'prep_project'),
    ('prep_user', 'prep_user')
]) }}

 
, joined AS (

    SELECT
      'gitlab_dotcom'                                                                     AS merge_request_data_source,
      gitlab_dotcom_merge_requests.merge_request_id,
      gitlab_dotcom_merge_requests.merge_request_internal_id                              AS merge_request_iid,
      gitlab_dotcom_merge_requests.merge_request_state,
      gitlab_dotcom_merge_requests.merge_request_status,
      gitlab_dotcom_merge_requests.created_at,
      gitlab_dotcom_merge_requests.merged_at,
      prep_project.project_id,
      target_project.project_id                                                           AS target_project_id,
      author.user_id                                                                      AS author_id,
      assignee.user_id                                                                    AS assignee_id,
      gitlab_dotcom_merge_requests.is_part_of_product,
      IFF(target_project.project_id = 14274989,1,0)                                       AS people_engineering_project,
      mapped_employee.bamboohr_employee_id,
      employee_directory.division,
      employee_directory.department
    FROM gitlab_dotcom_merge_requests
    INNER JOIN mapped_employee
      ON gitlab_dotcom_merge_requests.author_id = mapped_employee.gitlab_dotcom_user_id
    LEFT JOIN employee_directory
      ON mapped_employee.bamboohr_employee_id = employee_directory.employee_id
      AND DATE_TRUNC(day, gitlab_dotcom_merge_requests.merged_at) = employee_directory.date_actual
    LEFT JOIN prep_project
      ON gitlab_dotcom_merge_requests.dim_project_sk = prep_project.dim_project_sk
    LEFT JOIN prep_project AS target_project
      ON gitlab_dotcom_merge_requests.dim_project_sk = target_project.dim_project_sk
    LEFT JOIN prep_user AS author
      ON gitlab_dotcom_merge_requests.dim_user_author_sk = author.dim_user_sk
    LEFT JOIN prep_user AS assignee
      ON gitlab_dotcom_merge_requests.dim_user_assignee_sk = assignee.dim_user_sk

    UNION ALL  
    
    SELECT
      'gitlab_ops'                                                                       AS merge_request_data_source,
      merge_request_id,
      merge_request_iid,
      merge_request_state,
      merge_request_status,
      created_at,
      merged_at,
      project_id,
      target_project_id,
      author_id,
      assignee_id,
      is_part_of_product_ops,
      IFF(target_project_id = 14274989,1,0)                                              AS people_engineering_project,
      mapped_employee.bamboohr_employee_id,
      employee_directory.division,
      employee_directory.department
    FROM gitlab_ops_merge_requests
    INNER JOIN mapped_employee
      ON gitlab_ops_merge_requests.author_id = mapped_employee.gitlab_ops_user_id
    LEFT JOIN employee_directory
      ON mapped_employee.bamboohr_employee_id = employee_directory.employee_id
      AND DATE_TRUNC(day, gitlab_ops_merge_requests.merged_at) = employee_directory.date_actual
)

SELECT *
FROM joined

