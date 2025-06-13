{{ config(
    tags=["product"]
) }}

{{ simple_cte ([
  ('marketing_contact', 'dim_marketing_contact'),
  ('marketing_contact_order', 'bdg_marketing_contact_order'),
  ('dim_namespace', 'dim_namespace'),
  ('gitlab_dotcom_namespaces_source', 'gitlab_dotcom_namespaces_source'),
  ('gitlab_dotcom_users_source', 'gitlab_dotcom_users_source'),
  ('gitlab_dotcom_members_source', 'gitlab_dotcom_members_source'),
  ('gitlab_dotcom_memberships', 'gitlab_dotcom_memberships'),
  ('dim_subscription', 'dim_subscription'),
  ('dim_product_tier', 'dim_product_tier'),
  ('fct_trial_first', 'fct_trial_first'),
  ('customers_db_leads', 'customers_db_leads_source'),
  ('fct_event_user_daily', 'fct_event_user_daily'),
  ('map_gitlab_dotcom_xmau_metrics', 'map_gitlab_dotcom_xmau_metrics'),
  ('services', 'gitlab_dotcom_integrations_source'),
  ('project', 'prep_project'),
  ('ptp_scores_by_user', 'prep_ptp_scores_by_user'),
  ('namespace_details', 'gitlab_dotcom_namespace_details_source'),
  ('prep_ptpt_scores_by_user', 'prep_ptpt_scores_by_user')
]) }}

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
      AND dim_product_tier.product_tier_name IN ('SaaS - Ultimate','SaaS - Premium')
    GROUP BY 1

), latest_trial_by_user AS (

    SELECT *
    FROM fct_trial_first
    QUALIFY ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY trial_start_date DESC) = 1

), pqls AS (

    SELECT DISTINCT
      leads.product_interaction,
      leads.user_id,
      users.email,
      leads.namespace_id           AS dim_namespace_id,
      dim_namespace.namespace_name,
      leads.trial_start_date::DATE AS trial_start_date,
      leads.created_at             AS pql_event_created_at
    FROM customers_db_leads leads
    LEFT JOIN gitlab_dotcom_users_source AS users
      ON leads.user_id = users.user_id
    LEFT JOIN dim_namespace
      ON dim_namespace.dim_namespace_id = leads.namespace_id
    WHERE LOWER(leads.product_interaction) = 'hand raise pql'

    UNION ALL

    SELECT DISTINCT
      leads.product_interaction,
      leads.user_id,
      users.email,
      latest_trial_by_user.dim_namespace_id       AS dim_namespace_id,
      dim_namespace.namespace_name,
      latest_trial_by_user.trial_start_date::DATE AS trial_start_date,
      leads.created_at                            AS pql_event_created_at
    FROM customers_db_leads AS leads
    LEFT JOIN gitlab_dotcom_users_source AS users
      ON leads.user_id = users.user_id
    LEFT JOIN latest_trial_by_user
      ON latest_trial_by_user.user_id = leads.user_id
    LEFT JOIN dim_namespace
      ON dim_namespace.dim_namespace_id = leads.namespace_id
    WHERE LOWER(leads.product_interaction) = 'saas trial'
      AND leads.is_for_business_use = 'True'

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

), pqls_with_product_information AS (

    SELECT
      pqls.email,
      pqls.product_interaction                                             AS pql_product_interaction,
      COALESCE(pqls.dim_namespace_id,stages_adopted.dim_namespace_id)::INT AS pql_namespace_id,
      COALESCE(pqls.namespace_name,stages_adopted.namespace_name)          AS pql_namespace_name_masked,
      pqls.user_id,
      pqls.trial_start_date                                                AS pql_trial_start_date,
      stages_adopted.min_subscription_start_date                           AS pql_min_subscription_start_date,
      stages_adopted.list_of_stages                                        AS pql_list_stages,
      stages_adopted.active_stage_count                                    AS pql_nbr_stages,
      IFNULL(namespaces_with_user_count.current_member_count, 0) + 1       AS pql_nbr_namespace_users,
      pqls.pql_event_created_at
    FROM pqls
    LEFT JOIN stages_adopted
      ON pqls.dim_namespace_id = stages_adopted.dim_namespace_id
    LEFT JOIN namespaces_with_user_count
      ON namespaces_with_user_count.dim_namespace_id = pqls.dim_namespace_id
    WHERE LOWER(pqls.product_interaction) = 'saas trial'
      AND IFNULL(stages_adopted.min_subscription_start_date,CURRENT_DATE) >= pqls.trial_start_date

    UNION ALL

    SELECT
      pqls.email,
      pqls.product_interaction                                             AS pql_product_interaction,
      COALESCE(pqls.dim_namespace_id,stages_adopted.dim_namespace_id)::INT AS pql_namespace_id,
      COALESCE(pqls.namespace_name,stages_adopted.namespace_name)          AS pql_namespace_name_masked,
      pqls.user_id,
      pqls.trial_start_date                                                AS pql_trial_start_date,
      stages_adopted.min_subscription_start_date                           AS pql_min_subscription_start_date,
      stages_adopted.list_of_stages                                        AS pql_list_stages,
      stages_adopted.active_stage_count                                    AS pql_nbr_stages,
      IFNULL(namespaces_with_user_count.current_member_count, 0) + 1       AS pql_nbr_namespace_users,
      pqls.pql_event_created_at
    FROM pqls
    LEFT JOIN stages_adopted
      ON pqls.dim_namespace_id = stages_adopted.dim_namespace_id
    LEFT JOIN namespaces_with_user_count
      ON namespaces_with_user_count.dim_namespace_id = pqls.dim_namespace_id
    WHERE LOWER(pqls.product_interaction) = 'hand raise pql'

), latest_pql AS (

    SELECT
      pqls_with_product_information.*,
      gitlab_dotcom_namespaces_source.namespace_name                        AS pql_namespace_name
    FROM pqls_with_product_information
    LEFT JOIN gitlab_dotcom_namespaces_source
      ON gitlab_dotcom_namespaces_source.namespace_id = pqls_with_product_information.pql_namespace_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email ORDER BY pql_event_created_at DESC) = 1

), services_by_email AS (

    SELECT
      latest_pql.email                                                                           AS email,
      COUNT(*)                                                                                   AS pql_nbr_integrations_installed,
      ARRAY_AGG(DISTINCT services.integration_type) WITHIN GROUP (ORDER BY services.integration_type)    AS pql_integrations_installed
    FROM services
    LEFT JOIN project
      ON services.project_id = project.dim_project_id
    LEFT JOIN latest_pql
      ON latest_pql.pql_namespace_id = project.dim_namespace_id
    GROUP BY 1

), users_role_by_email AS (

    SELECT
      latest_pql.email,
      marketing_contact.job_title AS pql_namespace_creator_job_description
    FROM latest_pql
    INNER JOIN dim_namespace
      ON latest_pql.pql_namespace_id = dim_namespace.dim_namespace_id
    INNER JOIN marketing_contact
      ON dim_namespace.creator_id = marketing_contact.gitlab_dotcom_user_id

)
-------------------------- End of PQL logic --------------------------

, subscription_aggregate AS (

    SELECT
      dim_marketing_contact_id,
      MIN(subscription_start_date)                                                               AS min_subscription_start_date,
      MAX(subscription_end_date)                                                                 AS max_subscription_end_date
    FROM marketing_contact_order
    WHERE subscription_start_date is not null
    GROUP BY dim_marketing_contact_id

), namespace_notifications AS (

    SELECT
      email                               AS email_address,
      namespace_details.namespace_id      AS user_limit_namespace_id,
      dashboard_notification_at           AS user_limit_notification_at,
      dashboard_enforcement_at            AS user_limit_enforcement_at
    FROM namespace_details
    INNER JOIN dim_namespace
      ON namespace_details.namespace_id = dim_namespace.dim_namespace_id
    INNER JOIN gitlab_dotcom_users_source AS user
      ON user.user_id = dim_namespace.creator_id
      AND state = 'active'
    WHERE dashboard_notification_at IS NOT NULL OR dashboard_enforcement_at IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email_address ORDER BY COALESCE(dashboard_notification_at, dashboard_enforcement_at) ASC) = 1

), paid_subscription_aggregate AS (

    SELECT
      dim_marketing_contact_id,
      COUNT(DISTINCT dim_subscription_id)                                                        AS nbr_of_paid_subscriptions
    FROM marketing_contact_order
    WHERE dim_subscription_id is not null
      AND (is_saas_bronze_tier
           OR is_saas_premium_tier
           OR is_saas_ultimate_tier
           OR is_self_managed_starter_tier
           OR is_self_managed_premium_tier
           OR is_self_managed_ultimate_tier
          )
    GROUP BY dim_marketing_contact_id

), distinct_contact_subscription AS (

    SELECT DISTINCT
      dim_marketing_contact_id,
      dim_subscription_id,
      usage_umau_28_days_user,
      usage_action_monthly_active_users_project_repo_28_days_user,
      usage_merge_requests_28_days_user,
      usage_commit_comment_all_time_event,
      usage_source_code_pushes_all_time_event,
      usage_ci_pipelines_28_days_user,
      usage_ci_internal_pipelines_28_days_user,
      usage_ci_builds_28_days_user,
      usage_ci_builds_all_time_user,
      usage_ci_builds_all_time_event,
      usage_ci_runners_all_time_event,
      usage_auto_devops_enabled_all_time_event,
      usage_template_repositories_all_time_event,
      usage_ci_pipeline_config_repository_28_days_user,
      usage_user_unique_users_all_secure_scanners_28_days_user,
      usage_user_container_scanning_jobs_28_days_user,
      usage_user_sast_jobs_28_days_user,
      usage_user_dast_jobs_28_days_user,
      usage_user_dependency_scanning_jobs_28_days_user,
      usage_user_license_management_jobs_28_days_user,
      usage_user_secret_detection_jobs_28_days_user,
      usage_projects_with_packages_all_time_event,
      usage_projects_with_packages_28_days_event,
      usage_deployments_28_days_user,
      usage_releases_28_days_user,
      usage_epics_28_days_user,
      usage_issues_28_days_user,
      usage_instance_user_count_not_aligned,
      usage_historical_max_users_not_aligned
    FROM marketing_contact_order
    WHERE dim_subscription_id IS NOT NULL

), usage_metrics AS (

    SELECT
      dim_marketing_contact_id,
      SUM(usage_umau_28_days_user)                                                                  AS usage_umau_28_days_user,
      SUM(usage_action_monthly_active_users_project_repo_28_days_user)                              AS usage_action_monthly_active_users_project_repo_28_days_user,
      SUM(usage_merge_requests_28_days_user)                                                        AS usage_merge_requests_28_days_user,
      SUM(usage_commit_comment_all_time_event)                                                      AS usage_commit_comment_all_time_event,
      SUM(usage_source_code_pushes_all_time_event)                                                  AS usage_source_code_pushes_all_time_event,
      SUM(usage_ci_pipelines_28_days_user)                                                          AS usage_ci_pipelines_28_days_user,
      SUM(usage_ci_internal_pipelines_28_days_user)                                                 AS usage_ci_internal_pipelines_28_days_user,
      SUM(usage_ci_builds_28_days_user)                                                             AS usage_ci_builds_28_days_user,
      SUM(usage_ci_builds_all_time_user)                                                            AS usage_ci_builds_all_time_user,
      SUM(usage_ci_builds_all_time_event)                                                           AS usage_ci_builds_all_time_event,
      SUM(usage_ci_runners_all_time_event)                                                          AS usage_ci_runners_all_time_event,
      SUM(usage_auto_devops_enabled_all_time_event)                                                 AS usage_auto_devops_enabled_all_time_event,
      SUM(usage_template_repositories_all_time_event)                                               AS usage_template_repositories_all_time_event,
      SUM(usage_ci_pipeline_config_repository_28_days_user)                                         AS usage_ci_pipeline_config_repository_28_days_user,
      SUM(usage_user_unique_users_all_secure_scanners_28_days_user)                                 AS usage_user_unique_users_all_secure_scanners_28_days_user,
      SUM(usage_user_container_scanning_jobs_28_days_user)                                          AS usage_user_container_scanning_jobs_28_days_user,
      SUM(usage_user_sast_jobs_28_days_user)                                                        AS usage_user_sast_jobs_28_days_user,
      SUM(usage_user_dast_jobs_28_days_user)                                                        AS usage_user_dast_jobs_28_days_user,
      SUM(usage_user_dependency_scanning_jobs_28_days_user)                                         AS usage_user_dependency_scanning_jobs_28_days_user,
      SUM(usage_user_license_management_jobs_28_days_user)                                          AS usage_user_license_management_jobs_28_days_user,
      SUM(usage_user_secret_detection_jobs_28_days_user)                                            AS usage_user_secret_detection_jobs_28_days_user,
      SUM(usage_projects_with_packages_all_time_event)                                              AS usage_projects_with_packages_all_time_event,
      SUM(usage_projects_with_packages_28_days_event)                                               AS usage_projects_with_packages_28_days_event,
      SUM(usage_deployments_28_days_user)                                                           AS usage_deployments_28_days_user,
      SUM(usage_releases_28_days_user)                                                              AS usage_releases_28_days_user,
      SUM(usage_epics_28_days_user)                                                                 AS usage_epics_28_days_user,
      SUM(usage_issues_28_days_user)                                                                AS usage_issues_28_days_user,
      SUM(usage_instance_user_count_not_aligned)                                                    AS usage_instance_user_count_not_aligned,
      SUM(usage_historical_max_users_not_aligned)                                                   AS usage_historical_max_users_not_aligned
    FROM distinct_contact_subscription
    GROUP BY dim_marketing_contact_id

), last_ptpt_scores AS (

    SELECT *
    FROM prep_ptpt_scores_by_user
    WHERE score_date = (SELECT MAX(score_date) from prep_ptpt_scores_by_user)

), prep AS (

    SELECT
      marketing_contact.dim_marketing_contact_id,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.marketing_contact_role = 'Group Namespace Owner'
                    THEN 1
                  ELSE 0
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS is_group_namespace_owner,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN 1
                  ELSE 0
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS is_group_namespace_member,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.marketing_contact_role = 'Personal Namespace Owner'
                    THEN 1
                  ELSE 0
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS is_individual_namespace_owner,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Maintainer'
                                                                        )
                  AND (marketing_contact_order.is_saas_ultimate_tier OR marketing_contact_order.is_saas_premium_tier OR marketing_contact_order.is_saas_bronze_tier)
                    THEN 1
                  ELSE 0
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS is_group_maintainer_of_saas_paid_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Maintainer'
                                                                        )
                  AND marketing_contact_order.is_saas_free_tier
                    THEN 1
                  ELSE 0
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS is_group_maintainer_of_saas_free_tier,
      IFF(is_group_maintainer_of_saas_free_tier OR is_group_maintainer_of_saas_paid_tier, TRUE, FALSE) AS is_group_maintainer_of_saas,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.marketing_contact_role = 'Customer DB Owner'
                    THEN 1
                  ELSE 0
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS is_customer_db_owner,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.marketing_contact_role = 'Zuora Billing Contact'
                    THEN 1
                  ELSE 0
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS is_zuora_billing_contact,
      MIN(marketing_contact_order.days_since_saas_trial_ended)                                   AS days_since_saas_trial_ended,
      MIN(marketing_contact_order.days_since_saas_trial_ended_bucket)                            AS days_since_saas_trial_ended_bucket,
      MAX(marketing_contact_order.days_until_saas_trial_ends)                                    AS days_until_saas_trial_ends,
      MAX(marketing_contact_order.days_until_saas_trial_ends_bucket)                             AS days_until_saas_trial_ends_bucket,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 1
                    THEN marketing_contact_order.is_saas_trial AND marketing_contact_order.trial_end_date >= CURRENT_DATE
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS individual_namespace_is_saas_trial,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 1
                    THEN marketing_contact_order.is_saas_free_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS individual_namespace_is_saas_free_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 1
                    THEN marketing_contact_order.is_saas_bronze_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS individual_namespace_is_saas_bronze_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 1
                    THEN marketing_contact_order.is_saas_premium_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS individual_namespace_is_saas_premium_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 1
                    THEN marketing_contact_order.is_saas_ultimate_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS individual_namespace_is_saas_ultimate_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_group_namespace = 1
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN marketing_contact_order.is_saas_trial AND marketing_contact_order.trial_end_date >= CURRENT_DATE
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS group_member_of_saas_trial,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_group_namespace = 1
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN marketing_contact_order.is_saas_free_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS group_member_of_saas_free_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_group_namespace = 1
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN marketing_contact_order.is_saas_bronze_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS group_member_of_saas_bronze_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_group_namespace = 1
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN marketing_contact_order.is_saas_premium_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS group_member_of_saas_premium_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_group_namespace = 1
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN marketing_contact_order.is_saas_ultimate_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS group_member_of_saas_ultimate_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         )
                    THEN marketing_contact_order.is_saas_trial AND marketing_contact_order.trial_end_date >= CURRENT_DATE
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS group_owner_of_saas_trial,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         )
                    THEN marketing_contact_order.is_saas_free_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS group_owner_of_saas_free_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         )
                    THEN marketing_contact_order.is_saas_bronze_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS group_owner_of_saas_bronze_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         )
                    THEN marketing_contact_order.is_saas_premium_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS group_owner_of_saas_premium_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         )
                    THEN marketing_contact_order.is_saas_ultimate_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS group_owner_of_saas_ultimate_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         )
                    THEN marketing_contact_order.is_saas_trial AND marketing_contact_order.trial_end_date >= CURRENT_DATE
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS responsible_for_group_saas_trial,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         )
                    THEN marketing_contact_order.is_saas_free_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS responsible_for_group_saas_free_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         )
                    THEN marketing_contact_order.is_saas_bronze_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS responsible_for_group_saas_bronze_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         )
                    THEN marketing_contact_order.is_saas_premium_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS responsible_for_group_saas_premium_tier,
      CASE
        WHEN MAX(CASE
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         )
                    THEN marketing_contact_order.is_saas_ultimate_tier
                  ELSE NULL
                END) >= 1 THEN TRUE
        ELSE FALSE
      END                                                                                        AS responsible_for_group_saas_ultimate_tier,
      CASE
        WHEN MAX(marketing_contact_order.is_self_managed_starter_tier)  >= 1
          THEN TRUE
        ELSE FALSE
      END                                                                                        AS is_self_managed_starter_tier,
      CASE
        WHEN MAX(marketing_contact_order.is_self_managed_premium_tier)  >= 1
          THEN TRUE
        ELSE FALSE
      END                                                                                        AS is_self_managed_premium_tier,
      CASE
        WHEN MAX(marketing_contact_order.is_self_managed_ultimate_tier) >= 1
          THEN TRUE
        ELSE FALSE
      END                                                                                        AS is_self_managed_ultimate_tier,
      CASE
        WHEN MAX(marketing_contact_order.is_setup_for_company) = TRUE
          THEN TRUE
        ELSE FALSE
      END                                                                                        AS has_namespace_setup_for_company_use,
      CASE
        WHEN MAX(marketing_contact_order.does_namespace_have_public_project) = TRUE
          THEN TRUE
        ELSE FALSE
      END                                                                                        AS has_namespace_with_public_project,
      CASE
        WHEN MAX(marketing_contact_order.does_free_namespace_have_public_project) = TRUE
          THEN TRUE
        ELSE FALSE
      END                                                                                        AS has_free_namespace_with_public_project,
      CASE
        WHEN MAX(marketing_contact_order.is_ultimate_parent_namespace_public) = TRUE
          THEN TRUE
        ELSE FALSE
      END                                                                                        AS is_member_of_public_ultimate_parent_namespace,
      CASE
        WHEN MAX(marketing_contact_order.is_ultimate_parent_namespace_private) = TRUE
          THEN TRUE
        ELSE FALSE
      END                                                                                        AS is_member_of_private_ultimate_parent_namespace,
      ARRAY_AGG(DISTINCT IFF(marketing_contact_order.is_ultimate_parent_namespace_public = TRUE, marketing_contact_order.dim_namespace_id, NULL))
                                                                                                 AS public_ultimate_parent_namespaces,
      ARRAY_AGG(DISTINCT IFF(marketing_contact_order.is_ultimate_parent_namespace_private = TRUE, marketing_contact_order.dim_namespace_id, NULL))
                                                                                                 AS private_ultimate_parent_namespaces,
      ARRAY_AGG(
                DISTINCT
                CASE
                  WHEN marketing_contact_order.is_ultimate_parent_namespace = FALSE
                    THEN NULL
                  ELSE IFNULL(marketing_contact_order.marketing_contact_role || ': ' ||
                    IFNULL(marketing_contact_order.namespace_path, CASE
                                            WHEN marketing_contact_order.self_managed_product_tier IS NOT NULL
                                              THEN 'Self-Managed'
                                            ELSE ''
                                          END)  || ' | ' ||
                    IFNULL(marketing_contact_order.saas_product_tier, '') ||
                    IFNULL(marketing_contact_order.self_managed_product_tier, ''),

                    'No Namespace') END
               )                                                                                 AS role_tier_ultimate_namespace_text

    FROM marketing_contact
    LEFT JOIN  marketing_contact_order
      ON marketing_contact_order.dim_marketing_contact_id = marketing_contact.dim_marketing_contact_id
    GROUP BY marketing_contact.dim_marketing_contact_id

), joined AS (

    SELECT
      prep.*,
      IFF(individual_namespace_is_saas_bronze_tier
        OR group_owner_of_saas_bronze_tier
        OR group_member_of_saas_bronze_tier
        OR responsible_for_group_saas_bronze_tier,
        TRUE, FALSE)                                        AS is_saas_bronze_tier,
      IFF(individual_namespace_is_saas_premium_tier
        OR group_owner_of_saas_premium_tier
        OR group_member_of_saas_premium_tier
        OR responsible_for_group_saas_premium_tier,
        TRUE, FALSE)                                        AS is_saas_premium_tier,
      IFF(individual_namespace_is_saas_ultimate_tier
        OR group_owner_of_saas_ultimate_tier
        OR group_member_of_saas_ultimate_tier
        OR responsible_for_group_saas_ultimate_tier,
        TRUE, FALSE)                                        AS is_saas_ultimate_tier,
      IFF(is_saas_bronze_tier
        OR is_self_managed_starter_tier,
        TRUE, FALSE)                                        AS is_bronze_starter_tier,
      IFF(is_saas_premium_tier
        OR is_self_managed_premium_tier,
        TRUE, FALSE)                                        AS is_premium_tier,
      IFF(is_saas_ultimate_tier
        OR is_self_managed_ultimate_tier,
        TRUE, FALSE)                                        AS is_ultimate_tier,
      IFF(is_saas_bronze_tier
        OR is_saas_premium_tier
        OR is_saas_ultimate_tier,
        TRUE, FALSE)                                        AS is_saas_delivery,
      IFF(is_self_managed_starter_tier
        OR is_self_managed_premium_tier
        OR is_self_managed_ultimate_tier,
        TRUE, FALSE)                                        AS is_self_managed_delivery,
      IFF(individual_namespace_is_saas_free_tier
        OR group_member_of_saas_free_tier
        OR group_owner_of_saas_free_tier,
        TRUE, FALSE)                                        AS is_saas_free_tier,
      IFF(is_saas_delivery
        OR is_self_managed_delivery,
        TRUE, FALSE)                                        AS is_paid_tier,
      marketing_contact.is_paid_tier_marketo,
      IFF(is_paid_tier = TRUE OR (is_paid_tier = FALSE AND marketing_contact.is_paid_tier_marketo = TRUE), TRUE, FALSE)
                                                            AS is_paid_tier_change,
      subscription_aggregate.min_subscription_start_date,
      subscription_aggregate.max_subscription_end_date,
      paid_subscription_aggregate.nbr_of_paid_subscriptions,
      CASE
        WHEN (prep.responsible_for_group_saas_free_tier
              OR prep.individual_namespace_is_saas_free_tier
              OR prep.group_owner_of_saas_free_tier
             )
             AND NOT (prep.responsible_for_group_saas_ultimate_tier
                      OR prep.responsible_for_group_saas_premium_tier
                      OR prep.responsible_for_group_saas_bronze_tier
                      OR prep.individual_namespace_is_saas_bronze_tier
                      OR prep.individual_namespace_is_saas_premium_tier
                      OR prep.individual_namespace_is_saas_ultimate_tier
                      OR prep.group_owner_of_saas_bronze_tier
                      OR prep.group_owner_of_saas_premium_tier
                      OR prep.group_owner_of_saas_ultimate_tier
                     )
          THEN TRUE
        ELSE FALSE
      END                                                                                        AS responsible_for_free_tier_only,
      marketing_contact.is_in_embargoed_country,
      marketing_contact.email_address,
      marketing_contact.first_name,
      IFNULL(marketing_contact.last_name, 'Unknown')                                             AS last_name,
      marketing_contact.gitlab_user_name,
      IFNULL(marketing_contact.company_name, 'Unknown')                                          AS company_name,
      marketing_contact.sfdc_record_id,
      marketing_contact.dim_crm_account_id,
      marketing_contact.job_title,
      marketing_contact.contact_role,
      marketing_contact.it_job_title_hierarchy,
      marketing_contact.country,
      marketing_contact.mobile_phone,
      marketing_contact.sfdc_parent_sales_segment,
      marketing_contact.sfdc_parent_crm_account_region,
      marketing_contact.marketo_lead_id,
      marketing_contact.is_marketo_lead,
      marketing_contact.is_marketo_email_hard_bounced,
      marketing_contact.marketo_email_hard_bounced_date,
      marketing_contact.is_marketo_unsubscribed,
      marketing_contact.is_marketo_opted_in,
      marketing_contact.has_marketo_unsubscribed,
      marketing_contact.marketo_compliance_segment_value,
      marketing_contact.is_sfdc_lead_contact,
      marketing_contact.sfdc_lead_contact,
      marketing_contact.sfdc_created_date,
      marketing_contact.is_sfdc_opted_out,
      marketing_contact.is_gitlab_dotcom_user,
      marketing_contact.gitlab_dotcom_user_id,
      marketing_contact.gitlab_dotcom_created_date,
      marketing_contact.gitlab_dotcom_confirmed_date,
      marketing_contact.gitlab_dotcom_active_state,
      marketing_contact.gitlab_dotcom_last_login_date,
      marketing_contact.gitlab_dotcom_email_opted_in,
      marketing_contact.days_since_saas_signup,
      marketing_contact.days_since_saas_signup_bucket,
      marketing_contact.is_customer_db_user,
      marketing_contact.customer_db_customer_id,
      marketing_contact.customer_db_created_date,
      marketing_contact.customer_db_confirmed_date,
      IFF(latest_pql.email IS NOT NULL, TRUE, FALSE) AS is_pql,
      marketing_contact.is_pql_marketo,
      IFF(is_pql = TRUE OR (is_pql = FALSE AND marketing_contact.is_pql_marketo = TRUE), TRUE, FALSE)
                                            AS is_pql_change,
      latest_pql.pql_namespace_id,
      latest_pql.pql_namespace_name,
      latest_pql.pql_namespace_name_masked,
      latest_pql.pql_product_interaction,
      latest_pql.pql_list_stages,
      latest_pql.pql_nbr_stages,
      latest_pql.pql_nbr_namespace_users,
      latest_pql.pql_trial_start_date,
      latest_pql.pql_min_subscription_start_date,
      latest_pql.pql_event_created_at,
      services_by_email.pql_nbr_integrations_installed,
      services_by_email.pql_integrations_installed,
      users_role_by_email.pql_namespace_creator_job_description,
      marketing_contact.days_since_self_managed_owner_signup,
      marketing_contact.days_since_self_managed_owner_signup_bucket,
      marketing_contact.zuora_contact_id,
      marketing_contact.zuora_created_date,
      marketing_contact.zuora_active_state,
      marketing_contact.wip_is_valid_email_address,
      marketing_contact.wip_invalid_email_address_reason,

      IFF(contains(LOWER(marketing_contact.contact_role), 'gitlab admin') = TRUE, TRUE, FALSE)
                                                  AS is_gitlab_admin_in_sfdc,

      -- Propensity to purchase fields
      IFF(ptp_scores_by_user.model_grain_id IS NOT NULL, TRUE, FALSE)
                                                  AS is_ptp_contact,
      IFF(is_ptp_contact = TRUE OR (is_ptp_contact = FALSE AND marketing_contact.is_ptp_contact_marketo = TRUE
        ), TRUE, FALSE)
                                                  AS is_ptp_contact_change,
      IFF(ptp_scores_by_user.ptp_source IN ('Trial', 'Free'), ptp_scores_by_user.model_grain_id, NULL)
                                                  AS ptp_namespace_id,
      ptp_scores_by_user.score_group              AS ptp_score_group,
      ptp_scores_by_user.score_date               AS ptp_score_date,
      ptp_scores_by_user.insights                 AS ptp_insights,
      ptp_scores_by_user.past_insights            AS ptp_past_insights,
      ptp_scores_by_user.past_score_group         AS ptp_past_score_group,
      ptp_scores_by_user.past_score_date          AS ptp_past_score_date,
      CASE
        WHEN ptp_scores_by_user.days_since_trial_start BETWEEN 0 AND 30 THEN '< 30 days'
        WHEN ptp_scores_by_user.days_since_trial_start BETWEEN 30 AND 60 THEN '30 - 60 days'
        WHEN ptp_scores_by_user.days_since_trial_start BETWEEN 60 AND 90 THEN '60 - 90 days'
        WHEN ptp_scores_by_user.days_since_trial_start >= 90 THEN '90+ days'
      END                                         AS ptp_days_since_trial_start,
      ptp_scores_by_user.ptp_source               AS ptp_source,

       -- Propensity to purchase fields
      IFF(last_ptpt_scores.dim_marketing_contact_id IS NOT NULL, TRUE, FALSE)
                                                  AS is_ptpt_contact,
      IFF(is_ptpt_contact = TRUE OR (is_ptpt_contact = FALSE AND marketing_contact.is_ptpt_contact_marketo = TRUE
        ), TRUE, FALSE)
                                                  AS is_ptpt_contact_change,
      last_ptpt_scores.namespace_id               AS ptpt_namespace_id,
      last_ptpt_scores.score_group                AS ptpt_score_group,
      last_ptpt_scores.score_date                 AS ptpt_score_date,
      last_ptpt_scores.insights                   AS ptpt_insights,

      -- Namespace notification dates
      namespace_notifications.user_limit_namespace_id,
      namespace_notifications.user_limit_notification_at,
      namespace_notifications.user_limit_enforcement_at,
      IFF(namespace_notifications.user_limit_notification_at IS NOT NULL OR namespace_notifications.user_limit_enforcement_at IS NOT NULL,
        TRUE, FALSE)                              AS is_impacted_by_user_limit,
      IFF(is_impacted_by_user_limit = TRUE OR (is_impacted_by_user_limit = FALSE AND marketing_contact.is_impacted_by_user_limit_marketo = TRUE), TRUE, FALSE)
                                                  AS is_impacted_by_user_limit_change,

      usage_metrics.usage_umau_28_days_user,
      usage_metrics.usage_action_monthly_active_users_project_repo_28_days_user,
      usage_metrics.usage_merge_requests_28_days_user,
      usage_metrics.usage_commit_comment_all_time_event,
      usage_metrics.usage_source_code_pushes_all_time_event,
      usage_metrics.usage_ci_pipelines_28_days_user,
      usage_metrics.usage_ci_internal_pipelines_28_days_user,
      usage_metrics.usage_ci_builds_28_days_user,
      usage_metrics.usage_ci_builds_all_time_user,
      usage_metrics.usage_ci_builds_all_time_event,
      usage_metrics.usage_ci_runners_all_time_event,
      usage_metrics.usage_auto_devops_enabled_all_time_event,
      usage_metrics.usage_template_repositories_all_time_event,
      usage_metrics.usage_ci_pipeline_config_repository_28_days_user,
      usage_metrics.usage_user_unique_users_all_secure_scanners_28_days_user,
      usage_metrics.usage_user_container_scanning_jobs_28_days_user,
      usage_metrics.usage_user_sast_jobs_28_days_user,
      usage_metrics.usage_user_dast_jobs_28_days_user,
      usage_metrics.usage_user_dependency_scanning_jobs_28_days_user,
      usage_metrics.usage_user_license_management_jobs_28_days_user,
      usage_metrics.usage_user_secret_detection_jobs_28_days_user,
      usage_metrics.usage_projects_with_packages_all_time_event,
      usage_metrics.usage_projects_with_packages_28_days_event,
      usage_metrics.usage_deployments_28_days_user,
      usage_metrics.usage_releases_28_days_user,
      usage_metrics.usage_epics_28_days_user,
      usage_metrics.usage_issues_28_days_user,
      usage_metrics.usage_instance_user_count_not_aligned,
      usage_metrics.usage_historical_max_users_not_aligned,
      'Raw'                                                                                      AS lead_status,
      'Snowflake Email Marketing Database'                                                       AS lead_source
    FROM prep
    LEFT JOIN marketing_contact
      ON marketing_contact.dim_marketing_contact_id = prep.dim_marketing_contact_id
    LEFT JOIN subscription_aggregate
      ON subscription_aggregate.dim_marketing_contact_id = marketing_contact.dim_marketing_contact_id
    LEFT JOIN paid_subscription_aggregate
      ON paid_subscription_aggregate.dim_marketing_contact_id = marketing_contact.dim_marketing_contact_id
    LEFT JOIN usage_metrics
      ON usage_metrics.dim_marketing_contact_id = prep.dim_marketing_contact_id
    LEFT JOIN latest_pql
      ON latest_pql.email = marketing_contact.email_address
    LEFT JOIN services_by_email
      ON services_by_email.email = marketing_contact.email_address
    LEFT JOIN users_role_by_email
      ON users_role_by_email.email = marketing_contact.email_address
    LEFT JOIN ptp_scores_by_user
      ON ptp_scores_by_user.dim_marketing_contact_id = marketing_contact.dim_marketing_contact_id
    LEFT JOIN last_ptpt_scores
      ON last_ptpt_scores.dim_marketing_contact_id = marketing_contact.dim_marketing_contact_id
    LEFT JOIN namespace_notifications
      ON namespace_notifications.email_address = marketing_contact.email_address
)

{{ hash_diff(
    cte_ref="joined",
    return_cte="final",
    columns=[
      'is_group_namespace_owner',
      'is_group_namespace_member',
      'is_individual_namespace_owner',
      'is_customer_db_owner',
      'is_zuora_billing_contact',
      'days_since_saas_trial_ended_bucket',
      'individual_namespace_is_saas_trial',
      'individual_namespace_is_saas_free_tier',
      'individual_namespace_is_saas_bronze_tier',
      'individual_namespace_is_saas_premium_tier',
      'individual_namespace_is_saas_ultimate_tier',
      'group_member_of_saas_trial',
      'group_member_of_saas_free_tier',
      'group_member_of_saas_bronze_tier',
      'group_member_of_saas_premium_tier',
      'group_member_of_saas_ultimate_tier',
      'group_owner_of_saas_trial',
      'group_owner_of_saas_free_tier',
      'group_owner_of_saas_bronze_tier',
      'group_owner_of_saas_premium_tier',
      'group_owner_of_saas_ultimate_tier',
      'responsible_for_group_saas_trial',
      'responsible_for_group_saas_free_tier',
      'responsible_for_group_saas_bronze_tier',
      'responsible_for_group_saas_premium_tier',
      'responsible_for_group_saas_ultimate_tier',
      'is_self_managed_starter_tier',
      'is_self_managed_premium_tier',
      'is_self_managed_ultimate_tier',
      'is_in_embargoed_country',
      'email_address',
      'first_name',
      'last_name',
      'gitlab_user_name',
      'company_name',
      'country',
      'is_gitlab_dotcom_user',
      'gitlab_dotcom_user_id',
      'gitlab_dotcom_created_date',
      'gitlab_dotcom_confirmed_date',
      'gitlab_dotcom_active_state',
      'gitlab_dotcom_last_login_date',
      'gitlab_dotcom_email_opted_in',
      'is_customer_db_user',
      'customer_db_customer_id',
      'customer_db_created_date',
      'customer_db_confirmed_date',
      'zuora_contact_id',
      'zuora_created_date',
      'zuora_active_state',
      'pql_list_stages',
      'pql_nbr_stages',
      'pql_nbr_namespace_users',
      'has_namespace_setup_for_company_use',
      'pql_namespace_id',
      'pql_namespace_name',
      'pql_nbr_integrations_installed',
      'pql_integrations_installed',
      'pql_namespace_creator_job_description',
      'is_pql',
      'is_paid_tier',
      'is_pql_change',
      'is_paid_tier_change',
      'is_ptpt_contact',
      'is_ptpt_contact_change',
      'ptpt_namespace_id',
      'ptpt_score_group',
      'ptpt_insights',
      'ptpt_score_date',
      'is_member_of_public_ultimate_parent_namespace',
      'is_member_of_private_ultimate_parent_namespace',
      'user_limit_notification_at',
      'user_limit_enforcement_at',
      'is_impacted_by_user_limit',
      'is_impacted_by_user_limit_change',
      'user_limit_namespace_id',
      'marketo_lead_id',
      'is_ptp_contact',
      'is_ptp_contact_change',
      'ptp_namespace_id',
      'ptp_score_group',
      'ptp_score_date',
      'ptp_insights',
      'ptp_past_insights',
      'ptp_past_score_group',
      'ptp_days_since_trial_start',
      'ptp_source',
      'is_group_maintainer_of_saas_paid_tier',
      'is_gitlab_admin_in_sfdc'
      ]
) }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@trevor31",
    updated_by="@rkohnke",
    created_date="2021-02-09",
    updated_date="2024-10-07"
) }}
