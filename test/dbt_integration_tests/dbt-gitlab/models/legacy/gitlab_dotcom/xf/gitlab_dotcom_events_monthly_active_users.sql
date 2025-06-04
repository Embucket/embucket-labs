

WITH days AS (

    SELECT DISTINCT
      date_day                       AS day,
      (date_day = last_day_of_month) AS is_last_day_of_month
    FROM {{ ref('date_details') }}
    WHERE date_day < CURRENT_DATE

), audit_events AS (

    SELECT DISTINCT
      author_id,
      TO_DATE(created_at) AS audit_event_day
    FROM {{ ref('gitlab_dotcom_audit_events') }}
    WHERE TRUE

), events AS (

    SELECT DISTINCT
      author_id,
      ultimate_parent_id,
      plan_id_at_event_date,
      plan_was_paid_at_event_date,
      TO_DATE(created_at) AS event_day
    FROM {{ ref('gitlab_dotcom_events') }} dotcom_events
    WHERE {{ filter_out_blocked_users('dotcom_events', 'author_id') }}

), audit_events_active_user AS (

    SELECT
      days.day,
      days.is_last_day_of_month,
      COUNT(DISTINCT author_id)   AS count_audit_events_active_users_last_28_days
    FROM days
      INNER JOIN audit_events
        ON audit_event_day BETWEEN DATEADD('day', -27, days.day) AND days.day
    GROUP BY
      days.day,
      days.is_last_day_of_month
    ORDER BY
      days.day

), events_active_user AS (

    SELECT DISTINCT
      days.day,
      days.is_last_day_of_month,
      events.plan_id_at_event_date,
      events.plan_was_paid_at_event_date,
      COUNT(DISTINCT author_id) OVER (PARTITION BY days.day)                                              AS count_events_active_users_last_28_days,
      COUNT(DISTINCT author_id) OVER (PARTITION BY days.day, events.plan_id_at_event_date)                AS count_events_active_users_last_28_days_by_plan_id,
      COUNT(DISTINCT ultimate_parent_id) OVER (PARTITION BY days.day, events.plan_id_at_event_date)       AS count_events_active_namespaces_last_28_days_by_plan_id,
      COUNT(DISTINCT author_id) OVER (PARTITION BY days.day, events.plan_was_paid_at_event_date)          AS count_events_active_users_last_28_days_by_plan_was_paid
    FROM days
      INNER JOIN events
        ON events.event_day BETWEEN DATEADD('day', -27, days.day) AND days.day
    ORDER BY
      days.day

), joined AS (

    SELECT DISTINCT
      audit_events_active_user.day,
      audit_events_active_user.is_last_day_of_month,
      audit_events_active_user.count_audit_events_active_users_last_28_days,
      events_active_user.plan_id_at_event_date,
      events_active_user.plan_was_paid_at_event_date,
      events_active_user.count_events_active_users_last_28_days,
      events_active_user.count_events_active_users_last_28_days_by_plan_id,
      events_active_user.count_events_active_namespaces_last_28_days_by_plan_id,
      events_active_user.count_events_active_users_last_28_days_by_plan_was_paid
    FROM audit_events_active_user
      LEFT JOIN events_active_user
        ON audit_events_active_user.day = events_active_user.day
)

SELECT *
FROM joined
