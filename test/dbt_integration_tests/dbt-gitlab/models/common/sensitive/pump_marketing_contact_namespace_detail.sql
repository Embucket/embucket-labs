{{ config(
    materialized='table'
) }}

{{ simple_cte([
    ('dim_marketing_contact','dim_marketing_contact'),
    ('gitlab_dotcom_members_source','gitlab_dotcom_members_source'),
    ('dim_namespace', 'dim_namespace'),
    ('ptpt_scores', 'ptpt_scores'),
    ('ptpf_scores', 'ptpf_scores'),
    ('prep_namespace_order_trial', 'prep_namespace_order_trial'),
    ('gitlab_dotcom_namespace_details_source', 'gitlab_dotcom_namespace_details_source'),
    ('gitlab_dotcom_users_source', 'gitlab_dotcom_users_source'),
    ('gitlab_dotcom_memberships', 'gitlab_dotcom_memberships'),
    ('fct_trial_first', 'fct_trial_first'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_product_tier', 'dim_product_tier'),
    ('prep_lead', 'prep_lead'),
    ('map_gitlab_dotcom_xmau_metrics', 'map_gitlab_dotcom_xmau_metrics'),
    ('services', 'gitlab_dotcom_integrations_source'),
    ('project', 'prep_project'),
    ('fct_event_user_daily', 'fct_event_user_daily')
]) }}

, ptpt_score_dates AS (
    
    SELECT DISTINCT score_date
    FROM ptpt_scores
  
), ptpt_last_dates AS (
  
    SELECT
      FIRST_VALUE(score_date) OVER(ORDER BY score_date DESC)  AS last_score_date,
      NTH_VALUE(score_date, 2) OVER(ORDER BY score_date DESC) AS second_last_score_date
    FROM ptpt_score_dates
    LIMIT 1

), ptpf_score_dates AS (
    
    SELECT DISTINCT score_date
    FROM ptpf_scores
  
), ptpf_last_dates AS (
  
    SELECT
      FIRST_VALUE(score_date) OVER(ORDER BY score_date DESC)  AS last_score_date,
      NTH_VALUE(score_date, 2) OVER(ORDER BY score_date DESC) AS second_last_score_date
    FROM ptpf_score_dates
    LIMIT 1

), namespace_user_mapping AS (

  SELECT DISTINCT
    gitlab_dotcom_members_source.user_id,
    gitlab_dotcom_members_source.source_id AS namespace_id,
    dim_namespace.ultimate_parent_namespace_id,
    gitlab_dotcom_members_source.access_level
  FROM gitlab_dotcom_members_source
  LEFT JOIN dim_namespace
    ON gitlab_dotcom_members_source.source_id = dim_namespace.dim_namespace_id
  WHERE gitlab_dotcom_members_source.is_currently_valid = TRUE
    AND gitlab_dotcom_members_source.member_source_type = 'Namespace'
    AND {{ filter_out_blocked_users('gitlab_dotcom_members_source', 'user_id') }}
    AND gitlab_dotcom_members_source.user_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER(PARTITION BY gitlab_dotcom_members_source.user_id, gitlab_dotcom_members_source.source_id
    ORDER BY gitlab_dotcom_members_source.access_level DESC) = 1 -- There are less than 100 cases where a user has more than one access level to
    -- the same namespace. This selects the highest of said access levels

)

-------------------------- Start of PQL logic: --------------------------

, namespaces AS (
  
    SELECT
      gitlab_dotcom_users_source.email,
      dim_namespace.dim_namespace_id,
      dim_namespace.dim_product_tier_id,
      dim_namespace.namespace_name,
      dim_namespace.created_at              AS namespace_created_at,
      dim_namespace.created_at::DATE        AS namespace_created_at_date,
      dim_namespace.gitlab_plan_title       AS plan_title,
      dim_namespace.creator_id,
      dim_namespace.current_member_count    AS member_count
    FROM dim_namespace
    LEFT JOIN gitlab_dotcom_users_source
      ON gitlab_dotcom_users_source.user_id = dim_namespace.creator_id
    WHERE dim_namespace.namespace_is_internal = FALSE
      AND LOWER(gitlab_dotcom_users_source.state) = 'active'
      AND LOWER(dim_namespace.namespace_type) = 'group'
      AND dim_namespace.ultimate_parent_namespace_id = dim_namespace.dim_namespace_id 
      AND date(dim_namespace.created_at) >= '2021-01-27'::DATE
  
), flattened_members AS (

    SELECT --flattening members table to 1 record per member_id
      members.user_id,
      members.source_id,
      members.invite_created_at,
      MIN(members.invite_accepted_at) AS invite_accepted_at
    FROM gitlab_dotcom_members_source members
    INNER JOIN namespaces --limit to just namespaces we care about
      ON members.source_id = namespaces.dim_namespace_id --same as namespace_id for group namespaces
    WHERE LOWER(members.member_source_type) = 'namespace' --only looking at namespace invites
      AND members.invite_created_at >= namespaces.namespace_created_at --invite created after namespace created
      AND IFNULL(members.invite_accepted_at, CURRENT_TIMESTAMP) >= members.invite_created_at --invite accepted after invite created (removes weird edge cases with imported projects, etc)
    {{ dbt_utils.group_by(3) }}

), invite_status AS (

    SELECT --pull in relevant namespace data, invite status, etc
      namespaces.dim_namespace_id,
      members.user_id,
      IFF(memberships.user_id IS NOT NULL, TRUE, FALSE) AS invite_was_successful --flag whether the user actually joined the namespace
    FROM flattened_members members
    JOIN namespaces
      ON members.source_id = namespaces.dim_namespace_id --same as namespace_id for group namespaces
      AND (invite_accepted_at IS NULL OR (TIMESTAMPDIFF(minute,invite_accepted_at,namespace_created_at) NOT IN (0,1,2))) = TRUE -- this blocks namespaces created within two minutes of the namespace creator accepting their invite
    LEFT JOIN gitlab_dotcom_memberships memberships --record added once invite is accepted/user has access
      ON members.user_id = memberships.user_id
      AND members.source_id = memberships.membership_source_id
      AND memberships.is_billable = TRUE
    WHERE members.user_id != namespaces.creator_id --not an "invite" if user created namespace

), namespaces_with_user_count AS (

    SELECT
      dim_namespace_id,
      COUNT(DISTINCT user_id) AS current_member_count
    FROM invite_status
    WHERE invite_was_successful = TRUE
    GROUP BY 1

), subscriptions AS (
  
    SELECT 
      dim_subscription.namespace_id::INT                                     AS namespace_id, 
      MIN(dim_subscription.subscription_start_date)                          AS min_subscription_start_date
    FROM dim_subscription
    INNER JOIN namespaces 
      ON dim_subscription.namespace_id = namespaces.dim_namespace_id
    INNER JOIN dim_product_tier
      ON namespaces.dim_product_tier_id = dim_product_tier.dim_product_tier_id
    WHERE dim_subscription.namespace_id IS NOT NULL
      AND dim_product_tier.product_tier_name IN ('SaaS - Ultimate','SaaS - Premium') -- changing to product category field, used by the charges table
    GROUP BY 1
  
), latest_trial_by_user AS (
  
    SELECT *
    FROM fct_trial_first
    QUALIFY ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY trial_start_date DESC, namespace_created_at DESC) = 1

), pqls AS (
  
    SELECT DISTINCT
      prep_lead.product_interaction,
      prep_lead.user_id,
      users.email,
      prep_lead.dim_namespace_id            AS dim_namespace_id,
      dim_namespace.namespace_name,
      prep_lead.trial_start_date::DATE      AS trial_start_date,
      prep_lead.created_at                  AS pql_event_created_at
    FROM  prep_lead
    LEFT JOIN gitlab_dotcom_users_source AS users
      ON prep_lead.user_id = users.user_id
    LEFT JOIN dim_namespace
      ON dim_namespace.dim_namespace_id = prep_lead.dim_namespace_id
    WHERE LOWER(prep_lead.product_interaction) = 'hand raise pql'
  
    UNION ALL
  
    SELECT DISTINCT 
      prep_lead.product_interaction,
      prep_lead.user_id,
      users.email,
      latest_trial_by_user.dim_namespace_id       AS dim_namespace_id,
      dim_namespace.namespace_name,
      latest_trial_by_user.trial_start_date::DATE AS trial_start_date,
      prep_lead.created_at                        AS pql_event_created_at
    FROM prep_lead
    LEFT JOIN gitlab_dotcom_users_source AS users
      ON prep_lead.user_id = users.user_id
    LEFT JOIN latest_trial_by_user
      ON latest_trial_by_user.user_id = prep_lead.user_id
    LEFT JOIN dim_namespace
      ON dim_namespace.dim_namespace_id = prep_lead.dim_namespace_id
    WHERE LOWER(prep_lead.product_interaction) = 'saas trial'
      AND prep_lead.is_for_business_use = 'True'

), stages_adopted AS (
  
    SELECT 
      namespaces.dim_namespace_id,
      namespaces.namespace_name,
      namespaces.email,
      namespaces.creator_id,
      namespaces.member_count,
      'SaaS Trial or Free'                       AS product_interaction,
      subscriptions.min_subscription_start_date,
      ARRAYAGG(DISTINCT events.stage_name)       AS list_of_stages,
      COUNT(DISTINCT events.stage_name)          AS active_stage_count
    FROM fct_event_user_daily   AS events
    INNER JOIN namespaces 
      ON namespaces.dim_namespace_id = events.dim_ultimate_parent_namespace_id 
    LEFT JOIN map_gitlab_dotcom_xmau_metrics AS xmau 
      ON xmau.common_events_to_include = events.event_name
    LEFT JOIN subscriptions 
      ON subscriptions.namespace_id = namespaces.dim_namespace_id
    WHERE days_since_namespace_creation_at_event_date BETWEEN 0 AND 365
      AND events.plan_name_at_event_date IN ('trial','free', 'ultimate_trial') --Added in to only use events from a free or trial namespace (which filters based on the selection chose for the `free_or_trial` filter
      AND xmau.smau = TRUE
      AND events.event_date BETWEEN namespaces.namespace_created_at_date AND IFNULL(subscriptions.min_subscription_start_date,CURRENT_DATE)
    {{ dbt_utils.group_by(7) }}
  
), pqls_filtered AS (

    SELECT
      pqls.product_interaction                                             AS pql_product_interaction,
      COALESCE(pqls.dim_namespace_id,stages_adopted.dim_namespace_id)::INT AS pql_namespace_id,
      stages_adopted.min_subscription_start_date                           AS pql_min_subscription_start_date,
      pqls.pql_event_created_at
    FROM pqls
    LEFT JOIN stages_adopted 
      ON pqls.dim_namespace_id = stages_adopted.dim_namespace_id
    LEFT JOIN namespaces_with_user_count
      ON namespaces_with_user_count.dim_namespace_id = pqls.dim_namespace_id
    WHERE LOWER(pqls.product_interaction) = 'saas trial'
      AND IFNULL(stages_adopted.min_subscription_start_date,CURRENT_DATE) >= pqls.trial_start_date

    UNION

    SELECT 
      pqls.product_interaction                                             AS pql_product_interaction,
      COALESCE(pqls.dim_namespace_id,stages_adopted.dim_namespace_id)::INT AS pql_namespace_id,
      stages_adopted.min_subscription_start_date                           AS pql_min_subscription_start_date,
      pqls.pql_event_created_at
    FROM pqls
    LEFT JOIN stages_adopted
      ON pqls.dim_namespace_id = stages_adopted.dim_namespace_id
    LEFT JOIN namespaces_with_user_count
      ON namespaces_with_user_count.dim_namespace_id = pqls.dim_namespace_id
    WHERE LOWER(pqls.product_interaction) = 'hand raise pql'

), services_by_namespace AS (

    SELECT
      project.dim_namespace_id                                                                   AS dim_namespace_id,
      COUNT(*)                                                                                   AS nbr_integrations_installed,
      ARRAY_AGG(DISTINCT services.integration_type) WITHIN GROUP (ORDER BY services.integration_type)    AS integrations_installed
    FROM services
    LEFT JOIN project
      ON services.project_id = project.dim_project_id
    INNER JOIN namespaces
      ON namespaces.dim_namespace_id = project.dim_namespace_id
    GROUP BY 1

), pqls_by_user AS (

    SELECT DISTINCT user_id
    FROM pqls

)

-------------------------- End of PQL logic --------------------------

, stages_adopted_by_namespace AS (

  SELECT DISTINCT
    dim_namespace_id,
    list_of_stages,
    active_stage_count
  FROM stages_adopted

), namespace_details AS (

  SELECT
    namespace_user_mapping.user_id,
    dim_namespace.dim_namespace_id                                              AS namespace_id,
    namespace_user_mapping.access_level                                         AS user_access_level,
    CASE user_access_level
      WHEN 50 THEN 'Owner'
      WHEN 40 THEN 'Maintainer'
      WHEN 30 THEN 'Developer'
      WHEN 20 THEN 'Reporter'
      WHEN 10 THEN 'Guest'
      WHEN 5  THEN 'Minimal access'
      ELSE 'Other'
    END                                                                         AS user_access_level_name,

    dim_namespace.gitlab_plan_title,
    dim_namespace.gitlab_plan_is_paid,
    dim_namespace.is_setup_for_company,
    dim_namespace.current_member_count,
    dim_namespace.created_at                                                    AS created_at,
    dim_namespace.creator_id                                                    AS creator_user_id,

    prep_namespace_order_trial.order_start_date                                 AS trial_start_date,
    prep_namespace_order_trial.order_end_date                                   AS trial_expired_date,
    IFF(CURRENT_DATE() >= trial_start_date AND CURRENT_DATE() <= COALESCE(trial_expired_date, CURRENT_DATE()), TRUE, FALSE) 
                                                                                AS is_active_trial,
    prep_namespace_order_trial.trial_type,
    prep_namespace_order_trial.trial_type_name,
    prep_namespace_order_trial.glm_content,
    prep_namespace_order_trial.glm_source,

    IFF(pqls_filtered.pql_namespace_id IS NOT NULL, TRUE, FALSE)                AS is_namespace_pql,

    stages_adopted_by_namespace.list_of_stages,
    stages_adopted_by_namespace.active_stage_count,
    services_by_namespace.nbr_integrations_installed,
    services_by_namespace.integrations_installed,

    
    CASE
      WHEN last_ptpt_scores.score_group >= 4
        THEN 'Trial'
      WHEN last_ptpf_scores.score_group >= 4
        THEN 'Free'
      WHEN last_ptpt_scores.score_group IS NOT NULL
        THEN 'Trial'
      WHEN last_ptpf_scores.score_group IS NOT NULL
        THEN 'Free'
      ELSE NULL
    END                                                                         AS ptp_source,
    IFF(ptp_source = 'Trial', last_ptpt_scores.score_date, last_ptpf_scores.score_date)
                                                                                AS ptp_score_date,
    IFF(ptp_source = 'Trial', last_ptpt_scores.score_group, last_ptpf_scores.score_group)
                                                                                AS ptp_score_group,
    IFF(ptp_source = 'Trial', last_ptpt_scores.insights, last_ptpf_scores.insights)
                                                                                AS ptp_insights,
    IFF(ptp_source = 'Trial', second_last_ptpt_scores.score_group, second_last_ptpf_scores.score_group)
                                                                                AS ptp_previous_score_group,

    gitlab_dotcom_namespace_details_source.dashboard_notification_at            AS user_limit_notification_at,
    gitlab_dotcom_namespace_details_source.dashboard_enforcement_at             AS user_limit_enforcement_at,
    IFF(user_limit_notification_at IS NOT NULL OR user_limit_enforcement_at IS NOT NULL,
        TRUE, FALSE)                                                            AS is_impacted_by_user_limit

  FROM namespace_user_mapping
  INNER JOIN dim_namespace
    ON dim_namespace.dim_namespace_id = namespace_user_mapping.namespace_id
    AND namespace_is_ultimate_parent = TRUE
  LEFT JOIN ptpt_last_dates
  LEFT JOIN ptpt_scores AS last_ptpt_scores
    ON last_ptpt_scores.namespace_id = namespace_user_mapping.namespace_id
    AND last_ptpt_scores.score_date = ptpt_last_dates.last_score_date
  LEFT JOIN ptpt_scores AS second_last_ptpt_scores
    ON second_last_ptpt_scores.namespace_id = namespace_user_mapping.namespace_id
    AND second_last_ptpt_scores.score_date = ptpt_last_dates.second_last_score_date
  LEFT JOIN ptpf_last_dates
  LEFT JOIN ptpf_scores AS last_ptpf_scores
    ON last_ptpf_scores.namespace_id = namespace_user_mapping.namespace_id
    AND last_ptpf_scores.score_date = ptpf_last_dates.last_score_date
  LEFT JOIN ptpf_scores AS second_last_ptpf_scores
    ON second_last_ptpf_scores.namespace_id = namespace_user_mapping.namespace_id
    AND second_last_ptpf_scores.score_date = ptpf_last_dates.second_last_score_date
  LEFT JOIN prep_namespace_order_trial
    ON prep_namespace_order_trial.dim_namespace_id = namespace_user_mapping.namespace_id
  LEFT JOIN gitlab_dotcom_namespace_details_source
    ON gitlab_dotcom_namespace_details_source.namespace_id = namespace_user_mapping.namespace_id
  LEFT JOIN stages_adopted_by_namespace
    ON stages_adopted_by_namespace.dim_namespace_id = namespace_user_mapping.namespace_id
  LEFT JOIN pqls_filtered
    ON pqls_filtered.pql_namespace_id = namespace_user_mapping.namespace_id
  LEFT JOIN services_by_namespace
    ON services_by_namespace.dim_namespace_id = namespace_user_mapping.namespace_id

), user_aggregated_namespace_details AS (

  SELECT 
    user_id,
    ARRAY_AGG(OBJECT_CONSTRUCT(*)) AS namespaces_array
  FROM namespace_details
  GROUP BY 1
  
), user_trials AS (

  SELECT
    user_id,
    MAX(trial_start_date) AS max_trial_start_date
  FROM namespace_details
  GROUP BY 1

  ), user_details_and_namespace_details AS (

  SELECT
    dim_marketing_contact.dim_marketing_contact_id,
    dim_marketing_contact.email_address,
    dim_marketing_contact.first_name,
    dim_marketing_contact.last_name,
    dim_marketing_contact.country,
    dim_marketing_contact.company_name,
    dim_marketing_contact.job_title,
    dim_marketing_contact.gitlab_dotcom_user_id,
    dim_marketing_contact.gitlab_user_name,
    dim_marketing_contact.gitlab_dotcom_active_state,
    dim_marketing_contact.gitlab_dotcom_confirmed_date,
    dim_marketing_contact.gitlab_dotcom_created_date,
    dim_marketing_contact.gitlab_dotcom_last_login_date,
    dim_marketing_contact.gitlab_dotcom_email_opted_in,
    IFF(pqls_by_user.user_id IS NOT NULL, TRUE, FALSE) AS is_pql,
    user_trials.max_trial_start_date,
    user_aggregated_namespace_details.namespaces_array

  FROM dim_marketing_contact
  LEFT JOIN user_aggregated_namespace_details
    ON dim_marketing_contact.gitlab_dotcom_user_id = user_aggregated_namespace_details.user_id
  LEFT JOIN pqls_by_user
    ON pqls_by_user.user_id = dim_marketing_contact.gitlab_dotcom_user_id
  LEFT JOIN user_trials
    ON user_trials.user_id = dim_marketing_contact.gitlab_dotcom_user_id

  WHERE dim_marketing_contact.gitlab_dotcom_user_id IS NOT NULL
    AND (
      dim_marketing_contact.gitlab_dotcom_created_date::DATE >= '2022-06-01'
      OR user_trials.max_trial_start_date::DATE >= DATEADD('month', -3, CURRENT_DATE)
    )

)

{{ hash_diff(
    cte_ref="user_details_and_namespace_details",
    return_cte="final",
    columns=[
      'first_name',
      'last_name',
      'country',
      'company_name',
      'job_title',
      'gitlab_dotcom_user_id',
      'gitlab_user_name',
      'gitlab_dotcom_active_state',
      'gitlab_dotcom_confirmed_date',
      'gitlab_dotcom_created_date',
      'gitlab_dotcom_last_login_date',
      'gitlab_dotcom_email_opted_in',
      'is_pql',
      'max_trial_start_date',
      'namespaces_array'
      ]
) }}

SELECT *
FROM final
