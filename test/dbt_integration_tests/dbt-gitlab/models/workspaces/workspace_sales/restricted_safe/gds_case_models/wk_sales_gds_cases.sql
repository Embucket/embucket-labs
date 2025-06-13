/*
We pull all the Pooled cases, using the record type ID.

We have to manually parse the Subject field to get the Trigger Type, hopefully this will go away in future iterations.

-No spam filter
-Trigger Type logic only valid for FY25 onwards
*/

{{ simple_cte([
  ('prep_crm_case', 'prep_crm_case'),
  ('dim_crm_user', 'dim_crm_user'),
  ('dim_date', 'dim_date'),
  ('mart_crm_task', 'mart_crm_task'),
  ('mart_crm_event', 'mart_crm_event')
])
}},

case_data AS (
  SELECT
    CASE 
      WHEN prep_crm_case.subject LIKE 'Multiyear Renewal%' THEN 'Multiyear Renewal'
      WHEN prep_crm_case.subject LIKE 'EOA Renewal%' THEN 'EOA Renewal'
      WHEN prep_crm_case.subject LIKE 'PO Required%' THEN 'PO Required'
      WHEN prep_crm_case.subject LIKE 'Auto-Renewal Will Fail%' THEN 'Auto-Renewal Will Fail'
      WHEN prep_crm_case.subject LIKE 'Overdue Renewal%' THEN 'Overdue Renewal'
      WHEN prep_crm_case.subject LIKE 'Auto-Renew Recently Turned Off%' THEN 'Auto-Renew Recently Turned Off'
      WHEN prep_crm_case.subject LIKE 'Failed QSR%' THEN 'Failed QSR'
      ELSE prep_crm_case.subject
    END AS case_trigger,
    prep_crm_case.* EXCLUDE (created_by, updated_by, model_created_date, model_updated_date, dbt_created_at, dbt_updated_at),
    dim_crm_user.user_name AS case_owner_name,
    dim_crm_user.department AS case_department,
    dim_crm_user.team,
    dim_crm_user.manager_name,
    dim_crm_user.user_role_name AS case_user_role_name,
    dim_crm_user.user_role_type
  FROM prep_crm_case
  LEFT JOIN dim_crm_user
    ON prep_crm_case.dim_crm_user_id = dim_crm_user.dim_crm_user_id
  WHERE prep_crm_case.record_type_id = '0128X000001pPRkQAM'
    AND prep_crm_case.created_date >= '2024-02-01'
),

task_data_base AS (
  SELECT *
  FROM (
    SELECT
      task_id,
      task_status,
      task.dim_crm_account_id,
      task.dim_crm_user_id,
      task.dim_crm_person_id,
      task_type,
      task_subject,
      CASE
        WHEN task_subject LIKE '%Outreach%' AND task_subject NOT LIKE '%Advanced Outreach%' THEN 'Outreach'
        WHEN task_subject LIKE '%Clari%' THEN 'Clari'
        WHEN task_subject LIKE '%Conversica%' THEN 'Conversica'
        ELSE 'Other'
      END AS outreach_clari_flag,
      CASE
        WHEN (CONTAINS(LOWER(task_subject), '[inbox]') OR CONTAINS(LOWER(task_subject), '[flow]'))
          AND outreach_clari_flag = 'Other' THEN TRUE
        ELSE FALSE
      END AS groove_flag,
      CASE
        WHEN groove_flag = TRUE AND task_type = 'Call' THEN 'Groove Call'
        WHEN groove_flag = TRUE AND task_type = 'Email' AND CONTAINS(task_subject, '[Flow]') THEN 'Groove Flow Email'
        WHEN groove_flag = TRUE AND task_type = 'Email' AND CONTAINS(task_subject, '[Inbox]') THEN 'Groove Inbox Email'
        WHEN groove_flag = TRUE AND task_type = 'Call' AND CONTAINS(task_subject, 'Flow') THEN 'Groove Flow Call'
      END AS groove_task_category,
      CASE
        WHEN groove_flag = TRUE AND CONTAINS(LOWER(task_subject), '<<') THEN 'Inbound'
        WHEN groove_flag = TRUE AND CONTAINS(LOWER(task_subject), '>>') THEN 'Outbound'
        ELSE 'Other'
      END AS groove_inbound_outbound_flag,
      CASE
        WHEN CONTAINS(LOWER(task_subject), 'gong') AND outreach_clari_flag = 'Other' THEN TRUE
        ELSE FALSE
      END AS gong_flag,
      CASE
        WHEN gong_flag = TRUE AND task_type = 'Meeting' THEN TRUE
        WHEN CONTAINS(LOWER(task_subject), '(ii)') THEN TRUE
        WHEN CONTAINS(LOWER(task_subject), '#meeting') THEN TRUE
        WHEN task_type = 'Meeting' THEN TRUE
        ELSE FALSE
      END AS meeting_flag,
      task_created_date,
      task_created_by_id,
      CASE
        WHEN outreach_clari_flag = 'Outreach' AND (task_subject LIKE '%[Out]%' OR task_subject LIKE '%utbound%') THEN 'Outbound'
        WHEN outreach_clari_flag = 'Outreach' AND (task_subject LIKE '%[In]%' OR task_subject LIKE '%nbound%') THEN 'Inbound'
        WHEN outreach_clari_flag = 'Clari' AND CONTAINS(LOWER(task_subject), '[email received') THEN 'Inbound'
        WHEN outreach_clari_flag = 'Clari' AND CONTAINS(LOWER(task_subject), '[email sent') THEN 'Outbound'
        WHEN groove_inbound_outbound_flag = 'Inbound' THEN 'Inbound'
        WHEN groove_inbound_outbound_flag = 'Outbound' THEN 'Outbound'
        ELSE 'Other'
      END AS inbound_outbound_flag,
      COALESCE(
        (
          inbound_outbound_flag = 'Outbound'
          AND task_subject LIKE '%Answered%'
          AND task_subject NOT LIKE '%Not Answer%'
          AND task_subject NOT LIKE '%No Answer%'
        )
        OR (
          LOWER(task_subject) LIKE '%call%'
          AND task_subject NOT LIKE '%Outreach%'
          AND task_status = 'Completed'
        ),
        FALSE
      ) AS outbound_answered_flag,
      task_date,
      user.user_name AS task_user_name,
      CASE
        WHEN user.department LIKE '%arketin%' THEN 'Marketing'
        ELSE user.department
      END AS department,
      user.is_active,
      user.crm_user_sales_segment,
      user.crm_user_geo,
      user.crm_user_region,
      user.crm_user_area,
      user.crm_user_business_unit,
      user.user_role_name
    FROM mart_crm_task AS task
    INNER JOIN dim_crm_user AS user
      ON task.dim_crm_user_id = user.dim_crm_user_id
    WHERE task.dim_crm_user_id IS NOT NULL
      AND is_deleted = FALSE
      AND task_date >= '2024-02-01'
      AND task_status = 'Completed'

    UNION ALL

    SELECT 
      event_id AS task_id,
      'Completed' AS task_status,
      dim_crm_account_id,
      task.dim_crm_user_id,
      dim_crm_person_id,
      event_type AS task_type,
      event_subject AS task_subject,
      'Other' AS outreach_clari_flag,
      FALSE AS groove_flag,
      'Non-Groove - Event' AS groove_task_category,
      'Other' AS groove_inbound_outbound_flag,
      FALSE AS gong_flag,
      TRUE AS meeting_flag,
      event_created_date AS task_created_date,
      event_created_by_id AS task_created_by_id,
      'Other' AS inbound_outbound_flag,
      COALESCE(
        (
          inbound_outbound_flag = 'Outbound'
          AND task_subject LIKE '%Answered%'
          AND task_subject NOT LIKE '%Not Answer%'
          AND task_subject NOT LIKE '%No Answer%'
        )
        OR (
          LOWER(task_subject) LIKE '%call%'
          AND task_subject NOT LIKE '%Outreach%'
          AND task_status = 'Completed'
        ),
        FALSE
      ) AS outbound_answered_flag,
      event_date AS task_date,
      user.user_name AS task_user_name,
      CASE
        WHEN user.department LIKE '%arketin%' THEN 'Marketing'
        ELSE user.department
      END AS department,
      user.is_active,
      user.crm_user_sales_segment,
      user.crm_user_geo,
      user.crm_user_region,
      user.crm_user_area,
      user.crm_user_business_unit,
      user.user_role_name
    FROM mart_crm_event AS task
    INNER JOIN dim_crm_user AS user
      ON task.dim_crm_user_id = user.dim_crm_user_id 
    WHERE (
      LOWER(event_subject) LIKE '%#meeting'
      OR LOWER(event_subject) LIKE '%#meeting%'
      OR LOWER(event_subject) LIKE '#meeting%'
      OR LOWER(event_subject) LIKE '%(ii)%'
    )
    AND event_date >= '2024-02-01'
  )
  WHERE (
    user_role_name LIKE ANY ('%AE%', '%SDR%', '%BDR%', '%Advocate%')
    OR crm_user_sales_segment = 'SMB'
  )
),

task_data_prep AS (
  SELECT *,
    CASE 
      WHEN meeting_flag = TRUE THEN
        ROW_NUMBER() OVER (
          PARTITION BY dim_crm_account_id, task_date
          ORDER BY 
            task_created_date ASC,
            CASE 
              WHEN task_type = 'Meeting' THEN 1
              ELSE 2 
            END
        )
      ELSE 1
    END AS meeting_rank
  FROM task_data_base
),

task_data_prep_final AS (
  SELECT * 
  FROM task_data_prep
  WHERE meeting_rank = 1
),

task_data AS (
  SELECT * 
  FROM task_data_prep_final
  WHERE (
    user_role_name LIKE ANY ('%AE%', '%SDR%', '%BDR%', '%Advocate%')
    OR crm_user_sales_segment = 'SMB'
  )
),

case_task_summary_data AS (
  SELECT
    case_data.case_id AS case_task_summary_id,
    LISTAGG(DISTINCT task_data.task_id, ', ') AS case_task_id_list,
    COUNT(DISTINCT CASE WHEN task_data.inbound_outbound_flag = 'Inbound' THEN task_data.task_id END) AS inbound_email_count,
    COUNT(DISTINCT CASE WHEN task_data.outbound_answered_flag THEN task_data.task_id END) AS completed_call_count,
    COUNT(DISTINCT CASE WHEN task_data.inbound_outbound_flag = 'Outbound' THEN task_data.task_id END) AS outbound_email_count,
    COALESCE(inbound_email_count > 0, FALSE) AS task_inbound_flag,
    COALESCE(completed_call_count > 0, FALSE) AS task_completed_call_flag,
    COALESCE(outbound_email_count > 0, FALSE) AS task_outbound_flag,
    COUNT(DISTINCT task_data.task_id) AS completed_task_count,
    MIN(DATEDIFF('day', case_data.created_date, task_data.task_date)) AS days_to_first_task,
    MIN(DATEDIFF('day', task_data.task_date, CURRENT_DATE)) AS days_since_last_task
  FROM case_data
  LEFT JOIN task_data
    ON case_data.dim_crm_account_id = task_data.dim_crm_account_id
    AND task_data.task_date::DATE >= case_data.created_date::DATE
    AND (task_data.task_date::DATE <= case_data.closed_date::DATE OR case_data.closed_date IS NULL)
  GROUP BY 1
),

output AS (
  SELECT
    case_data.*,
    case_task_summary_data.* EXCLUDE (case_task_summary_id)
  FROM case_data
  LEFT JOIN case_task_summary_data 
    ON case_data.case_id = case_task_summary_data.case_task_summary_id
)

{{ dbt_audit(
    cte_ref="output",
    created_by="@mfleisher",
    updated_by="@mfleisher",
    created_date="2024-07-15",
    updated_date="2024-07-15"
) }}