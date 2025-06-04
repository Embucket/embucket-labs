{{ config(
    tags=["mnpi_exception"]
) }}

WITH prep_epic_user_request AS (

    SELECT *
    FROM {{ ref('prep_epic_user_request') }}

), prep_epic_user_request_collaboration_project AS (

    SELECT *
    FROM {{ ref('prep_epic_user_request_collaboration_project') }}

), epic_request_collaboration_projects_filtered AS (

    -- Epic request that are in the collaboration projects but are not in the Gitlab-org issue descriptions or notes

    SELECT prep_epic_user_request_collaboration_project.*
    FROM prep_epic_user_request_collaboration_project
    LEFT JOIN prep_epic_user_request
      ON prep_epic_user_request.epic_id = prep_epic_user_request_collaboration_project.epic_id
      AND prep_epic_user_request.dim_crm_account_id = prep_epic_user_request_collaboration_project.dim_crm_account_id
    WHERE prep_epic_user_request.epic_id IS NULL

), unioned AS (

    SELECT
      epic_id,
      dim_epic_sk,
      link_type,
      dim_crm_opportunity_id,
      dim_crm_account_id,
      dim_ticket_id,
      request_priority,
      is_request_priority_empty,
      FALSE                 AS is_user_request_only_in_collaboration_project,
      link_last_updated_at 
    FROM prep_epic_user_request

    UNION

    SELECT
      epic_id,
      dim_epic_sk,
      'Account'             AS link_type,
      MD5(-1)               AS dim_crm_opportunity_id,
      dim_crm_account_id,
      -1                    AS dim_ticket_id,
      1::NUMBER             AS request_priority,
      TRUE                  AS is_request_priority_empty,
      TRUE                  AS is_user_request_only_in_collaboration_project,
      link_last_updated_at
    FROM epic_request_collaboration_projects_filtered

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@jpeguero",
    updated_by="@michellecooper",
    created_date="2021-10-12",
    updated_date="2023-09-08",
) }}
