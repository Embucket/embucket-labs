WITH data_team_capacity AS (
  
  SELECT * 
  FROM {{ ref('sheetload_data_team_capacity') }}
  
), data_team_milestone_information AS (
  
  SELECT DISTINCT 
    namespace_id, 
    milestone_id, 
    milestone_title, 
    milestone_status,   
    start_date, 
    due_date
  FROM {{ ref('gitlab_dotcom_milestones_xf') }}
  WHERE namespace_id = '4347861'
    AND start_date > '2020-06-30' 
  
), final AS (
  
    SELECT 
      data_team_milestone_information.milestone_title     AS milestone_title, 
      data_team_milestone_information.start_date          AS milestone_start_date,
      data_team_milestone_information.due_date            AS milestone_due_date, 
      data_team_milestone_information.milestone_status    AS milestone_status, 
      data_team_capacity.gitlab_handle                    AS data_team_member_gitlab_handle, 
      data_team_capacity.capacity                         AS data_team_member_capacity
  FROM data_team_milestone_information
  LEFT JOIN data_team_capacity
      ON data_team_milestone_information.milestone_id = data_team_capacity.milestone_id
)

SELECT * 
FROM final 
