{{ config(
    materialized='incremental',
    tags=["mnpi_exception", "product"]
) }}

WITH alls AS (
    SELECT
        CASE 
            WHEN p.gsc_extra:new_nav = TRUE
            THEN 1
            ELSE 0 
        END AS using_new_nav,
        DATE_TRUNC(MONTH, p.behavior_at)::DATE AS page_view_month,
        COUNT(DISTINCT p.gsc_pseudonymized_user_id) AS unique_users,
        COUNT(DISTINCT p.session_id) AS sessions,
        COUNT(*) AS total_pageviews
    FROM
        {{ ref('mart_behavior_structured_event') }} AS p
    WHERE
        page_view_month < DATE_TRUNC(MONTH, CURRENT_DATE())
        AND
        p.behavior_at > '2022-06-01'
        AND
        p.app_id IN ('gitlab', 'gitlab_customers')

        {% if is_incremental() %}

            AND
            -- goal is to add entire new months only. 
            page_view_month > (SELECT MAX(page_view_month) FROM {{ this }})

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
        DATE_TRUNC(MONTH, mr.behavior_at)::DATE AS page_view_month,
        COUNT(DISTINCT mr.gsc_pseudonymized_user_id) AS users_count,
        COUNT(DISTINCT mr.session_id) AS sessions,
        COUNT(*) AS total_occurences
    FROM
        {{ ref('rpt_product_navigation_base') }} AS mr
    WHERE
        mr.behavior_at
        > DATEADD(DAY, 1, LAST_DAY(DATEADD(MONTH, -19, CURRENT_DATE())))
        AND
        mr.app_id IN ('gitlab', 'gitlab_customers')
        AND
        page_view_month < DATE_TRUNC(MONTH, CURRENT_DATE())
        {% if is_incremental() %}

            AND
            -- goal is to add entire new months only. 
            page_view_month > (SELECT MAX(page_view_month) FROM {{ this }})

        {% endif %}
    GROUP BY
        1, 2
)

SELECT
    alls.page_view_month,
    alls.using_new_nav,
    news.users_count AS nav_users,
    alls.unique_users,
    news.sessions AS nav_sessions,
    alls.sessions AS event_sessions,
    news.total_occurences AS nav_pageviews,
    alls.total_pageviews,
    news.users_count / alls.unique_users AS percent_users_on_navigation,
    news.sessions / alls.sessions AS ratio_navigations_to_sessions,
    news.total_occurences / alls.total_pageviews AS ratio_nav_views_to_all
FROM
    alls
LEFT JOIN
    news ON alls.page_view_month = news.page_view_month AND alls.using_new_nav = news.using_new_nav
