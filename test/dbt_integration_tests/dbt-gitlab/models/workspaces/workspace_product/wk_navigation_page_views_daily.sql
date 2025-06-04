{{ config(
    materialized='incremental',
    tags=["product"]
) }}

WITH navs AS (
    SELECT
        b.session_id,
        fct.dim_behavior_website_page_sk,
        behavior_date,
        CASE
            WHEN b.gsc_extra:new_nav = TRUE
                THEN 1
            ELSE 0
        END AS using_new_nav
    FROM
        {{ ref('rpt_product_navigation_base') }} AS b
            JOIN
        {{ ref('fct_behavior_structured_event_without_assignment_190') }} AS fct
        ON b.behavior_structured_event_pk = fct.behavior_structured_event_pk
    WHERE
        b.behavior_date > '2023-03-12'
        AND
        b.behavior_date < CURRENT_DATE
    GROUP BY
        1, 2, 3, 4
),

pvs AS (
    SELECT
        *,
        CASE
            WHEN p.gsc_extra:new_nav = TRUE
                THEN 1
            ELSE 0
        END AS using_new_nav
    FROM
        {{ ref('fct_behavior_website_page_view') }} AS p
        JOIN
        {{ ref('dim_behavior_website_page') }} AS d
        ON d.dim_behavior_website_page_sk = p.dim_behavior_website_page_sk
    WHERE
        p.behavior_at::DATE >= '2023-03-12'
        AND
        d.app_id IN ('gitlab', 'gitlab_customers')
        AND
        p.behavior_at::DATE < CURRENT_DATE
)


SELECT
    CASE
        WHEN p.using_new_nav = 1 THEN 'New Nav' ELSE 'Old Nav'
    END AS nav_type,
    p.behavior_at::DATE AS _date,
    COUNT(p.fct_behavior_website_page_view_sk) AS page_views,
    COUNT(n.dim_behavior_website_page_sk) AS nav_source_events,
    page_views - nav_source_events AS page_views_not_sourced_from_nav,
    100*(nav_source_events / page_views) AS percent_from_nav,
    100*(page_views_not_sourced_from_nav / page_views) AS percent_not_from_nav
FROM
    pvs AS p
LEFT JOIN
    navs AS n
    ON 
        p.behavior_at::DATE = n.behavior_date
        -- by joining referrer to page view we hope to capture whether preceding page had navigation events fire.
        AND n.dim_behavior_website_page_sk = p.dim_behavior_referrer_page_sk
        AND n.using_new_nav = p.using_new_nav
        AND p.session_id = n.session_id
{% if is_incremental() %}
    -- Goal is to only add complete days that are not currently added
    WHERE
        p.behavior_at::DATE > (SELECT MAX(_date) FROM {{ this }})
        AND
        p.behavior_at::DATE < CURRENT_DATE
{% endif %}
GROUP BY
    1, 2
