{{ config(
    materialized='incremental',
    tags=["product"]
) }}

WITH alls AS (
    SELECT
        CASE 
            WHEN p.gsc_extra:new_nav = TRUE
            THEN 1
            ELSE 0 
        END AS using_new_nav,
        DATE_TRUNC(WEEK, p.behavior_date)::DATE AS event_week,
        COUNT(DISTINCT p.gsc_pseudonymized_user_id) AS unique_users,
        COUNT(DISTINCT p.session_id) AS sessions,
        COUNT(*) AS total_pageviews
    FROM
        {{ ref('mart_behavior_structured_event') }} AS p
    WHERE
        event_week < DATE_TRUNC(WEEK, CURRENT_DATE())
        AND
        p.behavior_at > '2022-06-01'
        AND
        p.app_id IN ('gitlab', 'gitlab_customers')

        {% if is_incremental() %}

            AND
            -- goal is to add entire new weeks only. 
            event_week > (SELECT MAX(event_week) FROM {{ this }})

        {% endif %}

    GROUP BY 1,2
),

news AS (
    SELECT
        CASE 
            WHEN mr.gsc_extra:new_nav = TRUE
            THEN 1
            ELSE 0 
        END AS using_new_nav,
        DATE_TRUNC(WEEK, mr.behavior_date)::DATE AS event_week,
        COUNT(DISTINCT mr.gsc_pseudonymized_user_id) AS users_count,
        COUNT(DISTINCT mr.session_id) AS sessions,
        COUNT(*) AS total_occurences
    FROM
        {{ ref('rpt_product_navigation_base') }} AS mr
    WHERE
        mr.behavior_at
        > DATEADD(DAY, 1, LAST_DAY(DATEADD(WEEK, -19, CURRENT_DATE())))
        AND
        mr.app_id IN ('gitlab', 'gitlab_customers')
        AND
        event_week < DATE_TRUNC(WEEK, CURRENT_DATE())
        {% if is_incremental() %}

            AND
            -- goal is to add entire new weeks only. 
            event_week > (SELECT MAX(event_week) FROM {{ this }})

        {% endif %}
    GROUP BY
        1, 2
)

SELECT
    alls.event_week,
    alls.using_new_nav,
    news.users_count AS nav_users,
    alls.unique_users,
    news.sessions AS nav_sessions,
    alls.sessions AS event_sessions,
    news.total_occurences AS nav_pageviews,
    alls.total_pageviews,
    DIV0(news.users_count, alls.unique_users) AS percent_users_on_navigation,
    DIV0(news.sessions, alls.sessions) AS ratio_navigations_to_sessions,
    DIV0(news.total_occurences, alls.total_pageviews) AS ratio_nav_views_to_all
FROM
    alls
LEFT JOIN
    news ON alls.event_week = news.event_week AND alls.using_new_nav = news.using_new_nav
