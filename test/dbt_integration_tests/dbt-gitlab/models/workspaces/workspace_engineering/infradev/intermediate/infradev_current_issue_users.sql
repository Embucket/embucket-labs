{{ config(
    materialized='ephemeral'
) }}

WITH issue_assignees AS (
    
    SELECT *
    FROM {{ ref('gitlab_dotcom_issue_assignees_source') }} 

), users AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_users_dedupe_source') }} 

), assigend_users AS (
    
    SELECT
      issue_id                               AS issue_id,
      LISTAGG(DISTINCT users.username, ', ') AS assigned_usernames
    FROM issue_assignees
    LEFT JOIN users
      ON issue_assignees.user_id = users.id
    GROUP BY 1
)

  SELECT *
  FROM assigend_users