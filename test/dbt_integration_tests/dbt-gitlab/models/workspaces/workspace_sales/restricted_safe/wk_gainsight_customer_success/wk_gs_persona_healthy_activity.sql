{{ config(
    tags=["mnpi","gainsight"]
) }}

{{ simple_cte([
    ('wk_gs_activity_timeline', 'wk_gs_activity_timeline'),
    ('wk_gs_activity_attendee', 'wk_gs_activity_attendee'),
    ('wk_gs_company_person', 'wk_gs_company_person'),
    ('wk_gs_company', 'wk_gs_company'),
    ('wk_gs_success_plan', 'wk_gs_success_plan')
    ])

}}

,  joined AS (

    SELECT
      wk_gs_activity_timeline.activity_id,
      wk_gs_activity_timeline.account_id AS sfdc_account_id,
      wk_gs_activity_timeline.gs_company_id AS gs_company_id,
      wk_gs_activity_timeline.type_name,
      wk_gs_activity_timeline.ant_exec_sponsor_present_c,
      wk_gs_activity_timeline.activity_date::DATE AS activity_date,
      DATEDIFF('days', wk_gs_activity_timeline.activity_date, current_date) AS days_since_activity,
      wk_gs_activity_timeline.created_date::DATE AS created_date,
      wk_gs_activity_attendee.external_attendee_gs_id,
      wk_gs_activity_attendee.external_attendee_sfdc_id,
      wk_gs_activity_attendee.attendee_type,
      wk_gs_company_person.git_lab_role_gc                AS external_attendee_role,
      IFF(external_attendee_role LIKE '%Executive Sponsor%' AND days_since_activity <= 180, 1, 0)  AS exec_sponsor_healthy_activity,
      IFF(external_attendee_role LIKE '%Champion/Influencer%' AND days_since_activity <= 90, 1, 0) AS champion_healthy_activity,
      IFF(external_attendee_role LIKE '%Economic Buyer%' AND days_since_activity <= 180, 1, 0)     AS buyer_healthy_activity,
      IFF(external_attendee_role LIKE '%Security Lead%' AND days_since_activity <= 90, 1, 0)       AS security_lead_healthy_activity,
      IFF(external_attendee_role LIKE '%Development Lead%' AND days_since_activity <= 90, 1, 0)    AS dev_lead_healthy_activity,
      IFF(external_attendee_role LIKE '%Decision Maker%' AND days_since_activity <= 90, 1, 0)      AS decision_maker_healthy_activity,
      IFF(external_attendee_role LIKE '%Gitlab Admin%' AND days_since_activity <= 60, 1, 0)        AS admin_healthy_activity

    FROM wk_gs_activity_timeline
    INNER JOIN wk_gs_activity_attendee
      ON wk_gs_activity_attendee.activity_id = wk_gs_activity_timeline.activity_id
    INNER JOIN wk_gs_company_person
      ON wk_gs_company_person.GSID = wk_gs_activity_attendee.external_attendee_gs_id

    WHERE wk_gs_activity_attendee.attendee_type = 'EXTERNAL'
      AND wk_gs_company_person.git_lab_role_gc IS NOT NULL

),  activity AS (

      SELECT
        sfdc_account_id,
        gs_company_id,
        MAX(CASE WHEN external_attendee_role LIKE '%Executive Sponsor%' THEN activity_date END) AS most_recent_exec_activity,
        SUM(exec_sponsor_healthy_activity) AS exec_cnt,
        SUM(champion_healthy_activity) AS champion_cnt,
        SUM(buyer_healthy_activity) AS buyer_cnt,
        SUM(security_lead_healthy_activity) AS security_lead_cnt,
        SUM(dev_lead_healthy_activity) AS dev_lead_cnt,
        SUM(decision_maker_healthy_activity) AS decision_maker_cnt,
        SUM(admin_healthy_activity) AS admin_cnt
      FROM joined
      GROUP BY 1, 2

), last_workshop_delivered AS (

      SELECT
        wk_gs_activity_timeline.account_id AS sfdc_account_id,
        MAX(CASE WHEN type_name = 'Workshop' THEN activity_date END) AS last_workshop_delivered_date
        
      FROM wk_gs_activity_timeline
      GROUP BY 1

), success_plans AS (

    SELECT
      gsid AS success_plan_id,
      company_id,
      success_plan_link_gc,
      created_date
    FROM wk_gs_success_plan
    WHERE success_plan_link_gc IS NOT NULL
    AND is_closed = FALSE
    QUALIFY row_number() OVER (PARTITION BY COMPANY_ID ORDER BY created_date DESC) = 1

), lack_of_adoption_reasons AS (

    SELECT
      sfdc_account_id,
      lack_of_ci_adoption_reason_gc,
      lack_of_security_adoption_reason_gc
    FROM wk_gs_company
    WHERE lack_of_ci_adoption_reason_gc IS NOT NULL
    OR lack_of_security_adoption_reason_gc IS NOT NULL
)     
  SELECT
    activity.sfdc_account_id,
    gs_company_id,
    most_recent_exec_activity,
    last_workshop_delivered_date,
    lack_of_ci_adoption_reason_gc,
    lack_of_security_adoption_reason_gc,
    success_plan_link_gc,
    IFF(exec_cnt > 0, 1, 0) AS exec_persona_cnt,
    IFF(champion_cnt > 0, 1, 0) AS champion_persona_cnt,
    IFF(buyer_cnt > 0, 1, 0) AS buyer_persona_cnt,
    IFF(security_lead_cnt > 0, 1, 0) AS security_lead_persona_cnt,
    IFF(dev_lead_cnt > 0, 1, 0) AS dev_lead_persona_cnt,
    IFF(decision_maker_cnt > 0, 1, 0) AS decision_maker_persona_cnt,
    IFF(admin_cnt > 0, 1, 0) AS admin_persona_cnt,
    exec_persona_cnt
      + champion_persona_cnt
      + buyer_persona_cnt
      + security_lead_persona_cnt
      + dev_lead_persona_cnt
      + decision_maker_persona_cnt
      + admin_persona_cnt
      AS healthy_personas_cnt

  FROM activity
  LEFT JOIN last_workshop_delivered 
    ON last_workshop_delivered.sfdc_account_id = activity.sfdc_account_id
  LEFT JOIN success_plans 
    ON success_plans.company_id = activity.gs_company_id
  LEFT JOIN lack_of_adoption_reasons
    ON lack_of_adoption_reasons.sfdc_account_id = activity.sfdc_account_id
