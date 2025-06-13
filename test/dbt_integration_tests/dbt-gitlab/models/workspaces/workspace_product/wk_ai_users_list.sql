{{ config(
    materialized = 'table',
    tags = ["mnpi_exception", "product"]
) }} 

WITH prep AS (
    SELECT
        e.contexts:data [0] :data:feature_enabled_by_namespace_ids AS array_names,
        CASE
            WHEN e.dim_namespace_id IS NULL THEN 'Self-Managed'
            ELSE 'Gitlab.com'
        END AS likely_source,
        BEHAVIOR_STRUCTURED_EVENT_PK,
        DATE_TRUNC(MONTH, e.behavior_date) AS mmyyyy,
        e.behavior_date,
        gsc_is_gitlab_team_member,
        e.gsc_pseudonymized_user_id
    FROM
        {{ ref('mart_behavior_structured_event') }} e
    WHERE
        behavior_at BETWEEN '2024-03-21'
        AND CURRENT_DATE --first date of these events
        AND e.event_action = 'request_duo_chat_response'
),
unpacked AS (
    SELECT
        p.*,
        a.value AS namespace_id
    FROM
        prep p,
        LATERAL FLATTEN(array_names) AS a
),
duo_enabled AS (
    SELECT
        n.ultimate_parent_namespace_id,
        COUNT(DISTINCT gsc_pseudonymized_user_id) AS user_count_chat,
        COUNT(DISTINCT BEHAVIOR_STRUCTURED_EVENT_PK) AS event_count_chat
    FROM
        unpacked u
        JOIN {{ ref('dim_namespace') }} n ON u.namespace_id = n.dim_namespace_id
    WHERE
        u.GSC_IS_GITLAB_TEAM_MEMBER = 'false'
    GROUP BY
        ALL
),
cs_prep AS (
    SELECT
        a.value AS namespace_id,
        *
    FROM
        {{ ref('fct_behavior_structured_event_code_suggestion') }} c,
        LATERAL FLATTEN(c.ultimate_parent_namespace_ids) AS a
    WHERE
        c.namespace_is_internal != TRUE
),
cs AS (
    SELECT
        n.ultimate_parent_namespace_id,
        COUNT(DISTINCT GITLAB_GLOBAL_USER_ID) AS code_suggestion_user_count,
        COUNT(DISTINCT BEHAVIOR_STRUCTURED_EVENT_PK) AS code_suggestion_event_count
    FROM
        cs_prep
        JOIN {{ ref('dim_namespace') }} n ON n.dim_namespace_id = cs_prep.namespace_id
    GROUP BY
        ALL
),
mapper_ai AS (
    SELECT
        n.ultimate_parent_namespace_id,
        m.user_id AS owner_user_id,
        IFNULL(cs.ultimate_parent_namespace_id, FALSE) AS cs_enabled_and_use,
        IFNULL(d.ultimate_parent_namespace_id, FALSE) AS duo_enabled_and_use,
        COALESCE(cs.code_suggestion_user_count, 0) AS code_suggestion_user_count,
        COALESCE(cs.code_suggestion_event_count, 0) AS code_suggestion_event_count,
        COALESCE(d.user_count_chat, 0) AS user_count_chat,
        COALESCE(d.event_count_chat, 0) AS event_count_chat
    FROM
        {{ ref('dim_namespace') }} n
        LEFT JOIN cs ON cs.ultimate_parent_namespace_id = n.ultimate_parent_namespace_id
        LEFT JOIN duo_enabled d ON d.ultimate_parent_namespace_id = n.ultimate_parent_namespace_id
        JOIN {{ ref('gitlab_dotcom_members') }} m ON m.source_id = n.ultimate_parent_namespace_id
        AND m.access_level = 50
        AND m.is_currently_valid = TRUE
    WHERE
        n.namespace_is_ultimate_parent = TRUE
        AND n.namespace_is_internal != TRUE
        AND (
            cs.ultimate_parent_namespace_id IS NOT NULL
            OR d.ultimate_parent_namespace_id IS NOT NULL
        )
),
duo_purchasers AS (
    SELECT
        DISTINCT n.ultimate_parent_namespace_id,
    FROM
        {{ ref('mart_arr') }} AS mart_arr
        INNER JOIN {{ ref('dim_subscription') }} AS s -- joining to get namespace id because that identifier is not in mart_arr
        ON mart_arr.dim_subscription_id = s.dim_subscription_id
        JOIN {{ ref('dim_namespace') }} n ON n.dim_namespace_id = s.namespace_id
    WHERE
        arr_month >= '2024-02-01' -- first duo pro arr
        AND LOWER(mart_arr.product_deployment_type) = 'gitlab.com'
        AND LOWER(product_rate_plan_name) LIKE '%duo pro%'
), used_trial AS 
(
SELECT
DISTINCT 
n.ultimate_parent_namespace_id
FROM
{{ ref('fct_trial') }} t
JOIN
PROD.common.dim_namespace n ON n.dim_namespace_id = t.dim_namespace_id
WHERE 
product_rate_plan_id LIKE '%duo%pro%'
)

SELECT
    m.ultimate_parent_namespace_id,
    MAX(m.owner_user_id) AS owner_user_id,
    -- CHANGE THIS by removing the comments to deduplicate per namespace we can take the highest user id if you only want to connect to one member
    m.cs_enabled_and_use,
    m.duo_enabled_and_use,
    m.code_suggestion_user_count,
    m.code_suggestion_event_count,
    m.user_count_chat,
    m.event_count_chat,
    CASE
        WHEN m.cs_enabled_and_use = TRUE
        AND m.duo_enabled_and_use = TRUE THEN TRUE
        ELSE FALSE
    END AS used_both,
    CASE
        WHEN d.ultimate_parent_namespace_id IS NULL THEN 'Purchased Duo'
        WHEN t.ultimate_parent_namespace_id IS NOT NULL THEN 'Trial'
        ELSE 'Other'
    END AS trial_v_purchasers
FROM
    mapper_ai m
    LEFT JOIN duo_purchasers d ON d.ultimate_parent_namespace_id = m.ultimate_parent_namespace_id
    LEFT JOIN used_trial t ON t.ultimate_parent_namespace_id = m.ultimate_parent_namespace_id
GROUP BY
    ALL
ORDER BY
    USER_COUNT_CHAT DESC