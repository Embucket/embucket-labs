{{ config(materialized='table') }}

{{ simple_cte([
    ('mart_crm_opportunity','mart_crm_opportunity'),
    ('mart_crm_person','mart_crm_person'),
    ('mart_crm_account','mart_crm_account'),
    ('mart_crm_event','mart_crm_event'),
    ('mart_crm_task','mart_crm_task')
  ]) 
}}

, opportunity_base AS (
    SELECT 
        dim_crm_opportunity_id,
        dim_crm_account_id,
        dim_parent_crm_account_id,
        created_date,
        sales_accepted_date,
        pipeline_created_date,
        close_date,
        stage_name,
        stage_1_discovery_date,
        stage_2_scoping_date,
        stage_3_technical_evaluation_date,
        stage_4_proposal_date,
        subscription_type,
        opportunity_owner,
        DATEADD('day', -90, pipeline_created_date) AS pre_opportunity_window,
        COALESCE(close_date, CURRENT_DATE) AS activity_end_date
    FROM mart_crm_opportunity
    WHERE is_deleted = FALSE 
),

person_base AS (
    SELECT 
        dim_crm_person_id,
        sfdc_record_id,
        dim_crm_account_id,
        title,
        status,
        email_domain,
        email_domain_type,
        created_date AS person_created_date
    FROM mart_crm_person 
),

activity_base AS (
    SELECT 
        'task'              AS activity_type,
        dim_crm_task_sk     AS activity_id,
        dim_crm_opportunity_id,
        dim_crm_person_id,
        dim_crm_account_id,
        task_completed_date AS activity_date,
        task_type           AS activity_subtype,
        task_status         AS activity_status,
        dim_crm_user_id     AS activity_owner_user_id,
        CASE 
            WHEN is_meeting_task = 1 THEN 'Meeting'
            WHEN is_call_task = 1 THEN 'Call'
            WHEN is_email_task = 1 THEN 'Email'
            ELSE 'Other'
        END AS interaction_type,
        CASE
            WHEN is_opportunity_initiation_call_task = 1 
                 OR is_opportunity_initiation_email_task = 1 THEN 3
            WHEN is_opportunity_followup_call_task = 1 
                 OR is_opportunity_followup_email_task = 1 THEN 2
            ELSE 1
        END AS activity_importance,
        CASE
            WHEN is_answered_meaningfull_call_task = 1 THEN TRUE
            WHEN is_completed_task = 1 THEN TRUE
            ELSE FALSE
        END AS is_successful_interaction,
        CASE
            WHEN task_status = 'Completed' 
                 AND (is_opportunity_initiation_call_task = 1 
                      OR is_opportunity_initiation_email_task = 1)
            THEN TRUE
            ELSE FALSE
        END AS is_opportunity_driving_activity
    FROM mart_crm_task 
    WHERE 
        is_deleted = FALSE
        AND task_completed_date IS NOT NULL

    UNION ALL

    SELECT 
        'event'                                             AS activity_type,
        dim_crm_event_sk                                    AS activity_id,
        dim_crm_opportunity_id,
        dim_crm_person_id,
        dim_crm_account_id,
        event_date                                          AS activity_date,
        event_type                                          AS activity_subtype,
        NULL                                                AS activity_status,
        dim_crm_user_id                                     AS activity_owner_user_id,
        CASE
            WHEN outreach_meeting_type IS NOT NULL THEN outreach_meeting_type
            WHEN event_type IN ('Meeting', 'Call') THEN event_type
            ELSE 'Other'
        END AS interaction_type,
        CASE
            WHEN event_type = 'Meeting' 
                 AND event_end_date_time IS NOT NULL 
                 AND event_start_date_time IS NOT NULL THEN 3
            WHEN event_type = 'Call' THEN 2
            ELSE 1
        END AS activity_importance,
        CASE
            WHEN event_end_date_time IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_successful_interaction,
        CASE
            WHEN qualified_convo_or_meeting = 'Yes' THEN TRUE
            ELSE FALSE
        END AS is_opportunity_driving_activity
    FROM mart_crm_event
    WHERE event_date IS NOT NULL
),

contact_opportunity_relationship AS (
    SELECT 
        opportunity_base.dim_crm_opportunity_id,
        activity_base.dim_crm_person_id,
        person_base.sfdc_record_id,
        person_base.title AS contact_title,
        person_base.status AS contact_status,
        opportunity_base.created_date AS opportunity_created_date,
        opportunity_base.pipeline_created_date,
        opportunity_base.stage_name,
        opportunity_base.subscription_type,
        MIN(activity_base.activity_date) AS first_activity_date,
        MAX(activity_base.activity_date) AS last_activity_date,
        COUNT(DISTINCT activity_base.activity_id) AS total_activities,
        COUNT(DISTINCT CASE 
            WHEN activity_base.activity_date < opportunity_base.created_date 
            THEN activity_base.activity_id 
        END) AS pre_opportunity_activities,
        COUNT(DISTINCT CASE 
            WHEN activity_base.activity_date BETWEEN opportunity_base.created_date AND opportunity_base.activity_end_date
            THEN activity_base.activity_id 
        END) AS during_opportunity_activities,
        COUNT(DISTINCT CASE 
            WHEN activity_base.is_successful_interaction = TRUE 
            THEN activity_base.activity_id 
        END) AS successful_interactions,
        COUNT(DISTINCT CASE 
            WHEN activity_base.is_opportunity_driving_activity = TRUE 
            THEN activity_base.activity_id 
        END) AS opportunity_driving_activities,
        SUM(activity_base.activity_importance) AS total_activity_importance_score,
        DATEDIFF('day', MIN(activity_base.activity_date), MAX(activity_base.activity_date)) AS engagement_span_days,
        COUNT(DISTINCT DATE_TRUNC('month', activity_base.activity_date)) AS months_with_activity,
        CASE 
            WHEN
                 MIN(activity_base.activity_date) < opportunity_base.created_date 
                 AND COUNT(DISTINCT CASE 
                     WHEN activity_base.is_opportunity_driving_activity = TRUE 
                     THEN activity_base.activity_id 
                 END) > 0
            THEN 'Primary Contact'
            WHEN COUNT(DISTINCT activity_base.activity_id) >= 3
            THEN 'Active Participant'
            WHEN COUNT(DISTINCT activity_base.activity_id) > 0
            THEN 'Engaged Contact'            
            ELSE 'Associated Contact'
        END AS relationship_classification,
        DATEDIFF('day', MIN(activity_base.activity_date), opportunity_base.created_date) AS days_engaged_before_opportunity,
        DATEDIFF('day', opportunity_base.created_date, MAX(activity_base.activity_date)) AS days_engaged_during_opportunity
    FROM activity_base 
    INNER JOIN opportunity_base 
        ON activity_base.dim_crm_account_id = opportunity_base.dim_crm_account_id
        AND activity_base.activity_date BETWEEN opportunity_base.pre_opportunity_window AND opportunity_base.activity_end_date
    INNER JOIN person_base 
        ON activity_base.dim_crm_person_id = person_base.dim_crm_person_id
    {{dbt_utils.group_by(n=9)}}

), final AS (
    SELECT 
        contact_opportunity_relationship.*,
        CASE 
            WHEN relationship_classification = 'Primary Contact' 
                AND successful_interactions >= 3
                AND opportunity_driving_activities >= 2
            THEN 'Very Strong'
            WHEN relationship_classification IN ('Primary Contact', 'Active Participant')
                AND successful_interactions >= 2
            THEN 'Strong'
            WHEN total_activities >= 3
                AND months_with_activity >= 2
            THEN 'Moderate'
            ELSE 'Limited'
        END AS engagement_strength,
        ROW_NUMBER() OVER (
            PARTITION BY dim_crm_opportunity_id 
            ORDER BY 
                CASE relationship_classification
                    WHEN 'Primary Contact' THEN 1
                    WHEN 'Active Participant' THEN 2
                    WHEN 'Engaged Contact' THEN 3
                    ELSE 4
                END,
                total_activity_importance_score DESC,
                successful_interactions DESC,
                total_activities DESC
        ) AS contact_priority_rank

    FROM contact_opportunity_relationship 
    )

{{ dbt_audit(
    cte_ref="final",
    created_by="@dmicovic",
    updated_by="@dmicovic",
    created_date="2024-12-30",
    updated_date="2024-12-30",
  ) }}