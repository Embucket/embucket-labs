{{ config(
    materialized="table"
) }}

{{ simple_cte([
    ('prep_namespace','prep_namespace'),
    ('namespace_segmentation_scores', 'namespace_segmentation_scores'), 
    ('mart_crm_account','mart_crm_account'),
    ('prep_namespace_order_trial', 'prep_namespace_order_trial'),
    ('fct_trial','fct_trial'),
    ('mart_event_namespace_daily', 'mart_event_namespace_daily'),
    ('sfdc_account_snapshots_source','sfdc_account_snapshots_source'),
    ('prep_member_accepted_invites', 'prep_member_accepted_invites'),
    ('dim_namespace_hist', 'dim_namespace_hist'),
    ('dim_user', 'dim_user'),
    ('prep_action', 'prep_action'),
    ('dim_subscription', 'dim_subscription'),
    ('mart_product_usage_health_score', 'mart_product_usage_health_score'),
    ('rpt_behavior_code_suggestion_outcome', 'rpt_behavior_code_suggestion_outcome'),
    ('dim_ci_pipeline', 'dim_ci_pipeline'),
    ('mart_charge', 'mart_charge'),
    ('prep_order', 'prep_order'),
    ('dim_integration', 'dim_integration')
]) }}

, fct_event_base AS (

  SELECT
    parent_id,
    event_created_at,
    namespace_created_at,
    event_id,
    dim_user_id,
    event_name
  FROM {{ ref('fct_event_valid') }}
  WHERE event_created_at <= DATEADD('days',14,namespace_created_at)
    AND event_name IN (
    'accept_invite',
    'epic_notes',
    'merge_request_creation',
    'merge_request_note_creation',
    'issue_note_creation',
    'todos',
    'successful_ci_pipeline_creation',
    'other_ci_build_creation'
    )

), trial_base AS (

    SELECT
        prep_namespace_order_trial.dim_namespace_id,
        mart_event_namespace_daily.dim_crm_account_id,
        prep_namespace_order_trial.trial_type_name,
        fct_trial.user_id,
        fct_trial.trial_start_date,
        fct_trial.trial_end_date,
        ARRAY_AGG(fct_trial.product_rate_plan_id) AS product_rate_plan_id_array
    FROM prep_namespace_order_trial
    LEFT JOIN fct_trial
        ON prep_namespace_order_trial.dim_namespace_id = fct_trial.dim_namespace_id
            AND prep_namespace_order_trial.order_start_date = fct_trial.trial_start_date
    LEFT JOIN mart_event_namespace_daily
        ON prep_namespace_order_trial.dim_namespace_id=mart_event_namespace_daily.dim_ultimate_parent_namespace_id
    {{dbt_utils.group_by(n=6)}}

), trial_final AS (

    SELECT
        trial_base.*,
        CASE
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'free-plan-id')
            THEN CASE
                WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'ultimate-saas-trial-plan-id')
                THEN 'Free Ultimate SaaS'
                WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'saas-gitlab-duo-pro-trial-plan-id')
                THEN 'Free Duo Pro'
                WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'saas-gitlab-duo-enterprise-trial-plan-id')
                THEN 'Free Duo Enterprise'
                WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'premium-saas-trial-plan-id')
                THEN 'Free SaaS'
                WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'ultimate-saas-trial-w-duo-enterprise-trial-plan-id')
                THEN 'Free SaaS With Duo'
                ELSE 'Free Something'
                END
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'ultimate-saas-trial-plan-id')
              THEN 'SaaS Ultimate'
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'saas-gitlab-duo-pro-trial-plan-id')
              THEN 'SaaS Duo'
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'saas-gitlab-duo-enterprise-trial-plan-id')
              THEN 'SaaS Duo Enterprise'
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'premium-saas-trial-plan-id')
              THEN 'SaaS Premium'
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'ultimate-saas-trial-paid-customer-w-duo-enterprise-trial-plan-id')
              THEN 'SaaS Ultimate with Duo Enterprise Paid'
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'ultimate-saas-trial-paid-customer-plan-id')
              THEN 'SaaS Ultimate Paid'
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), 'ultimate-saas-trial-w-duo-enterprise-trial-plan-id')
              THEN 'SaaS Ultimate with Duo Enterprise'
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), '2c92a0fe6145beb901614f137a0f1685')
              THEN 'Ultimate Self-Managed'
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), '2c92a0ff6145d07001614f0ca2796525')
              THEN 'Premium Self-Managed'
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), '2c92a0ff76f0d5250176f2f8c86f305a')
              THEN 'Ultimate SaaS'
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), '2c92a00d76f0d5060176f2fb0a5029ff')
              THEN 'Premium SaaS'
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), '2c92a0fc5a83f01d015aa6db83c45aac')
              THEN 'Gold Plan - aka Ultimate for GitLab.com'
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), '2c92a0fd5a840403015aa6d9ea2c46d6')
              THEN 'Silver Plan - aka Premium for GitLab.com'
            WHEN CONTAINS(ARRAY_TO_STRING(product_rate_plan_id_array,','), '2c92a0ff5a840412015aa3cde86f2ba6')
              THEN 'Bronze Plan - aka Starter for GitLab.com'
            ELSE 'Not Grouped'
            END AS trial_type,
            sfdc_account_snapshots_source.account_type,
            'Trial' AS event_name
    FROM trial_base
    LEFT JOIN sfdc_account_snapshots_source
        ON trial_base.trial_start_date BETWEEN sfdc_account_snapshots_source.dbt_valid_from AND sfdc_account_snapshots_source.dbt_valid_to
            AND trial_base.dim_crm_account_id=sfdc_account_snapshots_source.account_id

), namespace_invites AS (

    SELECT
        'Namespace Invite' AS event_name,
        prep_member_accepted_invites.dim_member_id,
        prep_member_accepted_invites.dim_namespace_id,
        prep_member_accepted_invites.dim_user_id,
        prep_member_accepted_invites.created_at,
        dim_namespace_hist.namespace_type
    FROM prep_member_accepted_invites
    LEFT JOIN dim_namespace_hist
        ON prep_member_accepted_invites.dim_namespace_id=dim_namespace_hist.dim_namespace_id
            AND prep_member_accepted_invites.created_at BETWEEN dim_namespace_hist.valid_from AND dim_namespace_hist.valid_to

), second_member_added AS (

    SELECT
        '2nd User Added' AS event_name,
        parent_id,
        event_created_at,
        namespace_created_at,
        event_id,
        dim_user_id
    FROM fct_event_base
    WHERE event_name = 'accept_invite'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY parent_id ORDER BY event_created_at) = 2
        
), valuable_signup AS (

    SELECT
        'Valuable Signup' AS event_name,
        dim_user.dim_user_id,
        prep_action.dim_namespace_id,
        dim_subscription.dim_crm_account_id,
        dim_user.created_at::DATE AS event_date
    FROM dim_user
    LEFT JOIN prep_action
        ON dim_user.dim_user_id=prep_action.dim_user_id
    LEFT JOIN dim_subscription
        ON prep_action.dim_namespace_id=dim_subscription.namespace_id
    WHERE dim_user.is_valuable_signup = TRUE

), scm_score AS (

    SELECT
        'SCM Score' AS event_name,
        dim_namespace_id,
        dim_crm_account_id,
        scm_score,
        ping_created_at::DATE AS event_date
    FROM mart_product_usage_health_score
    WHERE scm_score IS NOT NULL

), cicd_pipeline_failure AS (

    SELECT
        'CI/CD Pipe Failure' AS event_name,
        dim_ci_pipeline.dim_namespace_id,
        dim_ci_pipeline.dim_user_id,
        dim_ci_pipeline.updated_at::DATE AS event_date,
        dim_subscription.dim_crm_account_id
    FROM dim_ci_pipeline
    LEFT JOIN dim_subscription
        ON dim_ci_pipeline.dim_namespace_id=dim_subscription.namespace_id
    WHERE status = 'failed'

), duo_license AS (

    SELECT
        'Duo License' AS event_name,
        mart_charge.dim_namespace_id,
        mart_charge.dim_crm_account_id,
        mart_charge.charge_created_date::DATE AS event_date,
        prep_order.user_id AS dim_user_id,
        mart_charge.product_rate_plan_name    
    FROM mart_charge
    LEFT JOIN prep_order
        ON mart_charge.dim_subscription_id=prep_order.dim_subscription_id
            AND mart_charge.dim_namespace_id=prep_order.dim_namespace_id
    WHERE LOWER(product_rate_plan_name) LIKE '%duo%'

), last_login AS (

    SELECT
        'Last Login' AS event_name,
        dim_user.last_sign_in_at::DATE AS event_date,
        prep_action.dim_namespace_id,
        dim_user.dim_user_id,
        dim_subscription.dim_crm_account_id
    FROM dim_user
    LEFT JOIN prep_action
        ON dim_user.dim_user_id=prep_action.dim_user_id
    LEFT JOIN dim_subscription
        ON prep_action.dim_namespace_id=dim_subscription.namespace_id

), integration_base AS (

    SELECT DISTINCT
        'Integration' AS event_name,
        dim_integration.integration_type,
        dim_integration.created_at::DATE AS event_date,
        prep_namespace.namespace_id AS dim_namespace_id,
        dim_subscription.dim_crm_account_id
    FROM dim_integration
    LEFT JOIN prep_namespace
        ON dim_integration.ultimate_parent_namespace_id=prep_namespace.namespace_id
    LEFT JOIN dim_subscription
        ON prep_namespace.namespace_id=dim_subscription.namespace_id
    WHERE is_active = TRUE
    
), events_unioned AS (

    SELECT
        CASE
          WHEN event_name = 'epic_notes'
            THEN 'Epic Note Creation'
          WHEN event_name = 'merge_request_creation'
            THEN 'MR Creation'
          WHEN event_name = 'merge_request_note_creation'
            THEN 'MR Note Creation'
          WHEN event_name = 'issue_note_creation'
            THEN 'Issue Note Creation'
          WHEN event_name = 'todos'
            THEN 'To Do Creation'
          WHEN event_name = 'successful_ci_pipeline_creation'
            THEN 'CI Pipe Creation'
          WHEN event_name = 'other_ci_build_creation'
            THEN 'Other CI Build Creation'
        END AS event_name,
        event_created_at,
        namespace_created_at,
        parent_id,
        event_id,
        dim_user_id
    FROM fct_event_base

), combined AS (

  SELECT
      trial_final.event_name,
      trial_final.dim_namespace_id,
      trial_final.user_id AS dim_user_id,
      trial_final.dim_crm_account_id,
      trial_final.account_type AS crm_account_type,
      trial_final.trial_start_date AS event_date,
      trial_final.trial_end_date,
      trial_final.trial_type,
      trial_final.trial_type_name,
      NULL AS product_rate_plan_name,
      NULL AS integration_type,
      NULL AS namespace_type
  FROM trial_final
  UNION ALL
  SELECT
      namespace_invites.event_name,
      namespace_invites.dim_namespace_id,
      namespace_invites.dim_user_id,
      namespace_segmentation_scores.dim_crm_account_id,
      NULL AS crm_account_type,
      namespace_invites.created_at,
      NULL AS trial_end_date,
      NULL AS trial_type,
      NULL AS trial_type_name,
      NULL AS product_rate_plan_name,
      NULL AS integration_type,
      namespace_invites.namespace_type
  FROM namespace_invites
  LEFT JOIN namespace_segmentation_scores
      ON namespace_invites.dim_namespace_id=namespace_segmentation_scores.dim_namespace_id
  UNION ALL
  SELECT
      second_member_added.event_name,
      prep_namespace.dim_namespace_id,
      second_member_added.dim_user_id,
      namespace_segmentation_scores.dim_crm_account_id,
      NULL AS crm_account_type,
      second_member_added.event_created_at,
      NULL AS trial_end_date,
      NULL AS trial_type,
      NULL AS trial_type_name,
      NULL AS product_rate_plan_name,
      NULL AS integration_type,
      NULL AS namespace_type
  FROM second_member_added
  LEFT JOIN prep_namespace
      ON second_member_added.parent_id=prep_namespace.ultimate_parent_namespace_id
          AND second_member_added.namespace_created_at=prep_namespace.created_at
  LEFT JOIN namespace_segmentation_scores
      ON prep_namespace.dim_namespace_id=namespace_segmentation_scores.dim_namespace_id
  UNION ALL
  SELECT
      events_unioned.event_name,
      prep_namespace.dim_namespace_id,
      events_unioned.dim_user_id,
      namespace_segmentation_scores.dim_crm_account_id,
      NULL AS crm_account_type,
      events_unioned.event_created_at,
      NULL AS trial_end_date,
      NULL AS trial_type,
      NULL AS trial_type_name,
      NULL AS product_rate_plan_name,
      NULL AS integration_type,
      NULL AS namespace_type
  FROM events_unioned
  LEFT JOIN prep_namespace
      ON events_unioned.parent_id=prep_namespace.ultimate_parent_namespace_id
          AND events_unioned.namespace_created_at=prep_namespace.created_at
  LEFT JOIN namespace_segmentation_scores
      ON prep_namespace.dim_namespace_id=namespace_segmentation_scores.dim_namespace_id
  UNION ALL
  SELECT
      valuable_signup.event_name,
      valuable_signup.dim_namespace_id,
      valuable_signup.dim_user_id,
      valuable_signup.dim_crm_account_id,
      NULL AS crm_account_type,
      valuable_signup.event_date,
      NULL AS trial_end_date,
      NULL AS trial_type,
      NULL AS trial_type_name,
      NULL AS product_rate_plan_name,
      NULL AS integration_type,
      NULL AS namespace_type
  FROM valuable_signup
  UNION ALL
  SELECT
      scm_score.event_name,
      scm_score.dim_namespace_id,
      NULL AS dim_user_id,
      scm_score.dim_crm_account_id,
      NULL AS crm_account_type,
      scm_score.event_date,
      NULL AS trial_end_date,
      NULL AS trial_type,
      NULL AS trial_type_name,
      NULL AS product_rate_plan_name,
      NULL AS integration_type,
      NULL AS namespace_type
  FROM scm_score
  UNION ALL
  SELECT
      cicd_pipeline_failure.event_name,
      cicd_pipeline_failure.dim_namespace_id,
      cicd_pipeline_failure.dim_user_id,
      cicd_pipeline_failure.dim_crm_account_id,
      NULL AS crm_account_type,
      cicd_pipeline_failure.event_date,
      NULL AS trial_end_date,
      NULL AS trial_type,
      NULL AS trial_type_name,
      NULL AS product_rate_plan_name,
      NULL AS integration_type,
      NULL AS namespace_type
  FROM cicd_pipeline_failure
  UNION ALL
  SELECT
      duo_license.event_name,
      duo_license.dim_namespace_id,
      duo_license.dim_user_id,
      duo_license.dim_crm_account_id,
      NULL AS crm_account_type,
      duo_license.event_date,
      NULL AS trial_end_date,
      NULL AS trial_type,
      NULL AS trial_type_name,
      duo_license.product_rate_plan_name,
      NULL AS integration_type,
      NULL AS namespace_type
  FROM duo_license
  UNION ALL
  SELECT
      last_login.event_name,
      last_login.dim_namespace_id,
      last_login.dim_user_id,
      last_login.dim_crm_account_id,
      NULL AS crm_account_type,
      last_login.event_date,
      NULL AS trial_end_date,
      NULL AS trial_type,
      NULL AS trial_type_name,
      NULL AS product_rate_plan_name,
      NULL AS integration_type,
      NULL AS namespace_type
  FROM last_login
  UNION ALL
  SELECT
      integration_base.event_name,
      integration_base.dim_namespace_id,
      NULL AS dim_user_id,
      integration_base.dim_crm_account_id,
      NULL AS crm_account_type,
      integration_base.event_date,
      NULL AS trial_end_date,
      NULL AS trial_type,
      NULL AS trial_type_name,
      NULL AS product_rate_plan_name,
      integration_base.integration_type,
      NULL AS namespace_type
  FROM integration_base

), final AS (

  SELECT
    combined.event_name,
    combined.dim_namespace_id,
    COALESCE(combined.namespace_type,prep_namespace.namespace_type) AS namespace_type,
    combined.dim_user_id,
    combined.dim_crm_account_id,
    COALESCE(combined.crm_account_type,mart_crm_account.crm_account_type) AS crm_account_type,
    combined.event_date,
    combined.trial_end_date,
    combined.trial_type_name,
    combined.trial_type,
    combined.product_rate_plan_name,
    combined.integration_type
  FROM combined
  LEFT JOIN mart_crm_account
      ON combined.dim_crm_account_id=mart_crm_account.dim_crm_account_id
  LEFT JOIN prep_namespace
        ON combined.dim_namespace_id=prep_namespace.dim_namespace_id

)

SELECT *
FROM final