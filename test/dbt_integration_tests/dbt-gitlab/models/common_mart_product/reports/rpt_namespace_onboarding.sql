{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
  ('dim_namespace','dim_namespace'),
  ('prep_gitlab_dotcom_plan', 'prep_gitlab_dotcom_plan'),
	('dim_user', 'dim_user'),
	('prep_lead', 'prep_lead'),
	('gitlab_dotcom_members', 'gitlab_dotcom_members'),
	('prep_namespace_order_trial', 'prep_namespace_order_trial'),
  ('mart_charge', 'mart_charge'),
  ('dim_product_tier', 'dim_product_tier'),
	('mart_event_namespace_daily', 'mart_event_namespace_daily'),
  ('gitlab_dotcom_memberships', 'gitlab_dotcom_memberships'),
	('mart_event_valid', 'mart_event_valid'),
	('fct_usage_storage', 'fct_usage_storage'),
  ('dim_marketing_contact_no_pii', 'dim_marketing_contact_no_pii'),
  ('mart_crm_person', 'mart_crm_person')
    ])
}},

namespaces AS ( --All currently existing namespaces within Gitlab.com. Filters out namespaces with blocked creators, and internal namespaces. Filtered to ultimate parent namespaces.

  SELECT
    dim_namespace.ultimate_parent_namespace_id, -- Keeping this id naming convention for clarity
    dim_product_tier_id,
    dim_namespace.created_at                                                                                AS namespace_created_at, --timestamp is useful for relative calculations - ex) file created win 1 minute of namespace creation
    dim_namespace.created_at::DATE                                                                          AS namespace_created_date,
    dim_namespace.creator_id,
    dim_namespace.namespace_type, -- Not limiting to Group namespaces only to facilitate broader analyses if needed
    dim_user.setup_for_company,
    dim_user.created_at::DATE                                                                               AS creator_created_date,
    dim_user.created_at                                                                                     AS creator_created_at, --timestamp is useful for registration_type timediff calculation
    dim_namespace.visibility_level,
    dim_namespace.gitlab_plan_id                                                                            AS current_gitlab_plan_id,
    plan.plan_name_modified                                                                                 AS current_gitlab_plan_title,
    dim_namespace.current_project_count,
    dim_namespace.is_deleted,
    dim_namespace.is_deleted_updated_at,
    DATEDIFF(DAY, namespace_created_date, CURRENT_DATE)                                                     AS days_since_namespace_creation,
    MIN(pql.created_at)::DATE                                                                               AS handraise_pql_date,
    MAX(IFF(TIMESTAMPDIFF(MINUTE, members.invite_accepted_at, namespace_created_at) BETWEEN 0 AND 2, 1, 0))
      AS is_namespace_created_within_2min_of_creator_invite_acceptance, --filterable field specific to growth conversion analysis
    MIN(members.invite_accepted_at)                                                                         AS creator_invite_accepted_at
  FROM dim_namespace
  INNER JOIN prep_gitlab_dotcom_plan AS plan
    ON dim_namespace.gitlab_plan_id = plan.dim_plan_id
  LEFT JOIN prep_lead AS pql
    ON dim_namespace.dim_namespace_id = pql.dim_namespace_id
      AND pql.product_interaction = 'Hand Raise PQL'
  LEFT JOIN dim_user
    ON dim_namespace.creator_id = dim_user.dim_user_id
  LEFT JOIN gitlab_dotcom_members AS members -- legacy schema
    ON dim_namespace.creator_id = members.user_id AND members.invite_accepted_at IS NOT NULL
  WHERE namespace_is_internal = FALSE
    AND namespace_creator_is_blocked = FALSE
    AND namespace_is_ultimate_parent = TRUE
  GROUP BY ALL

),

trials_prep AS ( -- Current trial data does not specify what type of trial was started

  SELECT DISTINCT
    namespaces.ultimate_parent_namespace_id,
    trials.order_start_date::DATE                                                                                      AS trial_start_date,
    trials.order_created_at                                                                                            AS trial_start_at, --timestamp is useful for registration_type
    trials.trial_type,
    trials.trial_type_name,
    DATEDIFF('days', namespace_created_date, trial_start_date)                                                         AS days_since_namespace_creation_at_trial,
    ROW_NUMBER() OVER (PARTITION BY namespaces.ultimate_parent_namespace_id ORDER BY trials.order_created_at) = 1      AS is_first_trial,
    ROW_NUMBER() OVER (PARTITION BY namespaces.ultimate_parent_namespace_id ORDER BY trials.order_created_at DESC) = 1 AS is_latest_trial
  FROM namespaces
  INNER JOIN prep_namespace_order_trial AS trials
    ON namespaces.ultimate_parent_namespace_id = trials.dim_namespace_id

),

trials_grouping AS (

  SELECT
    ultimate_parent_namespace_id,
    ARRAY_AGG(
      OBJECT_CONSTRUCT(
        'trial_type',
        trial_type,
        'trial_type_name',
        trial_type_name,
        'trial_start_date',
        trial_start_date,
        'days_since_namespace_creation_at_trial',
        days_since_namespace_creation_at_trial
      )
    ) WITHIN GROUP (
      ORDER BY
        trial_type
    ) AS all_trials
  FROM trials_prep
  GROUP BY ALL

),

trials AS (

  SELECT
    trials_prep.ultimate_parent_namespace_id,
    trials_grouping.all_trials,
    MAX(IFF(trials_prep.trial_type = 1, trials_prep.trial_type, NULL))                                     AS trial_type,
    MAX(IFF(trials_prep.trial_type = 1, trials_prep.trial_type_name, NULL))                                AS trial_type_name,
    MAX(IFF(trials_prep.trial_type = 1, trials_prep.trial_start_date, NULL))                               AS trial_start_date,
    MAX(IFF(trials_prep.trial_type = 1, trials_prep.days_since_namespace_creation_at_trial, NULL))         AS days_since_namespace_creation_at_trial,
    MAX(IFF(trials_prep.trial_type = 2, trials_prep.trial_type_name, NULL))                                AS trial_2_type_name,
    MAX(IFF(trials_prep.trial_type = 2, trials_prep.trial_start_date, NULL))                               AS trial_2_start_date,
    MAX(IFF(trials_prep.trial_type = 2, trials_prep.days_since_namespace_creation_at_trial, NULL))         AS days_since_namespace_creation_at_trial_2,
    MIN(trials_prep.trial_start_at)                                                                        AS first_trial_start_at, --used for registration_type logic
    MAX(IFF(trials_prep.is_first_trial = TRUE, trials_prep.trial_type, NULL))                              AS first_trial_type,
    MAX(IFF(trials_prep.is_first_trial = TRUE, trials_prep.trial_type_name, NULL))                         AS first_trial_type_name,
    MAX(IFF(trials_prep.is_first_trial = TRUE, trials_prep.trial_start_date, NULL))                        AS first_trial_start_date,
    MAX(IFF(trials_prep.is_first_trial = TRUE, trials_prep.days_since_namespace_creation_at_trial, NULL))  AS days_since_namespace_creation_at_first_trial,
    MAX(IFF(trials_prep.is_latest_trial = TRUE, trials_prep.trial_type, NULL))                             AS latest_trial_type,
    MAX(IFF(trials_prep.is_latest_trial = TRUE, trials_prep.trial_type_name, NULL))                        AS latest_trial_type_name,
    MAX(IFF(trials_prep.is_latest_trial = TRUE, trials_prep.trial_start_date, NULL))                       AS latest_trial_start_date,
    MAX(IFF(trials_prep.is_latest_trial = TRUE, trials_prep.days_since_namespace_creation_at_trial, NULL)) AS days_since_namespace_creation_at_latest_trial
  FROM trials_prep
  LEFT JOIN trials_grouping ON trials_prep.ultimate_parent_namespace_id = trials_grouping.ultimate_parent_namespace_id
  GROUP BY ALL

),

charges AS ( --First paid subscription for ultimate namespace

  SELECT DISTINCT
    namespaces.ultimate_parent_namespace_id,
    charges.subscription_start_date                                           AS first_paid_subscription_start_date,
    DATEDIFF('days', namespace_created_date, charges.subscription_start_date)
      AS days_since_namespace_creation_at_first_paid_subscription,
    SPLIT_PART(charges.product_tier_name, ' - ', 2)                           AS first_paid_plan_name, --product_category: SaaS - Premium, SaaS - Ultimate, etc
    IFF(
      charges.subscription_created_by_id IN ('2c92a0fd55822b4d015593ac264767f2', '2c92a0107bde3653017bf00cd8a86d5a'),
      TRUE, FALSE
    )                                                                         AS first_paid_plan_purchased_through_subscription_portal -- logic to be moved to fact_charge model
  FROM namespaces
  INNER JOIN mart_charge AS charges
    ON namespaces.ultimate_parent_namespace_id = charges.ultimate_parent_namespace_id
  WHERE charges.is_licensed_user_base_product = TRUE --exclude add-ons, support, etc
    AND charges.product_deployment_type = 'GitLab.com' --limit to GitLab.com only
    AND charges.dim_namespace_id IS NOT NULL
    AND charges.mrr > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY namespaces.ultimate_parent_namespace_id ORDER BY charges.subscription_start_date) = 1

),

activation_events AS ( --Event part of the activation definition occuring while namespace was free within 14 days of namespace creation

  SELECT
    namespaces.ultimate_parent_namespace_id,
    MIN(events.event_date)                                                                                      AS event_activation_date,
    MIN(events.days_since_namespace_creation_at_event_date)                                                     AS days_since_namespace_creation_at_activation_event,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT events.event_name) WITHIN GROUP (ORDER BY events.event_name ASC), ' , ')
      AS activation_event_array
  FROM namespaces
  INNER JOIN mart_event_namespace_daily AS events
    ON namespaces.ultimate_parent_namespace_id = events.dim_ultimate_parent_namespace_id
  WHERE LOWER(events.event_name) IN ('merge_request_note_creation', 'todos', 'other_ci_build_creation', 'successful_ci_pipeline_creation', 'issue_note_creation', 'merge_request_creation', 'epic_notes') -- activation events
    AND events.days_since_namespace_creation_at_event_date BETWEEN 0 AND 13
    AND events.plan_was_paid_at_event_date = FALSE --including activity prior to conversion
  GROUP BY 1

),

second_billable_member AS ( -- 2nd part of the activation definition - 2nd billable member added within 14 days
-- using a window function to calculate the cumulative number of new members added to a namespace within 14 days so that I can identify the # days it took for a namespace to add a 2nd user

  SELECT
    namespaces.ultimate_parent_namespace_id,
    DATEDIFF(DAY, namespace_created_date, dim_user.created_at::DATE)                                                                               AS days_since_namespace_creation_at_2nd_user_add, --2nd user add date
    dim_user.created_at::DATE                                                                                                                      AS second_user_created_date,
    COUNT(DISTINCT dim_user.dim_user_id)                                                                                                           AS count_billable_members, -- # of users added on the date of the 2nd add - could be 1 or 2 - field is not in final output
    SUM(count_billable_members) OVER (PARTITION BY namespaces.ultimate_parent_namespace_id ORDER BY days_since_namespace_creation_at_2nd_user_add) AS total_users_added   -- always 2 - field is not in final output
  FROM namespaces
  INNER JOIN gitlab_dotcom_memberships AS mships -- legacy shema
    ON namespaces.ultimate_parent_namespace_id = mships.ultimate_parent_id
  INNER JOIN dim_user -- including all users with a membership to the ultimate parent regardless of creator status
    ON mships.user_id = dim_user.dim_user_id
      AND dim_user.is_blocked_user = FALSE
      AND mships.is_billable = TRUE -- Optional based on use case - excluding guests, bots, other use cases that would not be counted towards a subscription if one were to exist
  LEFT JOIN charges -- joining charges info to exclude users joining namespaces with a subscription before they were created
    ON namespaces.ultimate_parent_namespace_id = charges.ultimate_parent_namespace_id
  WHERE COALESCE(charges.first_paid_subscription_start_date, CURRENT_DATE) >= dim_user.created_at::DATE -- excluding users joining namespaces with a subscription before they were created
    AND namespace_created_date <= dim_user.created_at::DATE -- excluding users created prior to namespace creation
    AND days_since_namespace_creation_at_2nd_user_add <= 13
  GROUP BY 1, 2, 3 --grouping by namespace and days since namespace creation when a user was added
  QUALIFY total_users_added = 2

),

d60_retention AS ( --return event between 60 - 90 days

  SELECT DISTINCT namespaces.ultimate_parent_namespace_id
  FROM namespaces
  INNER JOIN mart_event_namespace_daily AS events
    ON namespaces.ultimate_parent_namespace_id = events.dim_ultimate_parent_namespace_id
  WHERE days_since_namespace_creation_at_event_date BETWEEN 59 AND 89

),

first_last_activity AS ( --min and max event dates

  SELECT
    namespaces.ultimate_parent_namespace_id,
    MIN(event_date) AS min_event_date,
    MAX(event_date) AS max_event_date
  FROM namespaces
  INNER JOIN mart_event_namespace_daily AS events
    ON namespaces.ultimate_parent_namespace_id = events.dim_ultimate_parent_namespace_id
  WHERE days_since_namespace_creation_at_event_date >= 0
  GROUP BY 1

),

valuable_signup AS ( --counting namespaces with billable members who initially sign up with a business email domain and are created prior to any paid subscription

  SELECT
    namespaces.ultimate_parent_namespace_id,
    MAX(IFF(dim_user.dim_user_id = namespaces.creator_id, 1, 0)) AS creator_is_valuable_signup_numeric,
    IFF(creator_is_valuable_signup_numeric = 1, TRUE, FALSE)     AS creator_is_valuable_signup
  FROM namespaces
  INNER JOIN gitlab_dotcom_memberships AS mships
    ON namespaces.ultimate_parent_namespace_id = mships.ultimate_parent_id
  INNER JOIN dim_user -- including all users with a membership to the ultimate parent regardless of creator status
    ON mships.user_id = dim_user.dim_user_id
      AND dim_user.is_blocked_user = FALSE
      AND mships.is_billable = TRUE -- Optional based on use case - excluding guests, bots, other use cases that would not be counted towards a subscription if one were to exist
  LEFT JOIN charges -- joining charges info to exclude users joining namespaces with a subscription before they were created
    ON namespaces.ultimate_parent_namespace_id = charges.ultimate_parent_namespace_id
  WHERE COALESCE(charges.first_paid_subscription_start_date, CURRENT_DATE) >= dim_user.created_at::DATE -- excluding users joining namespaces with a subscription before they were created
    AND dim_user.email_domain_classification IS NULL -- indicates a business email domain
  GROUP BY 1

),

stage_adoption AS (

  SELECT
    namespaces.ultimate_parent_namespace_id,
    stage_name,
    MIN(days_since_namespace_creation_at_event_date)                                                                   AS days_since_namespace_creation_at_first_event_date,
    MAX(days_since_namespace_creation_at_event_date)                                                                   AS days_since_namespace_creation_at_latest_event_date,
    -- COUNT(DISTINCT days_since_namespace_creation_at_event_date)        AS stage_usage_days,
    -- COUNT(DISTINCT dim_user_id)                                        AS stage_users, --I don't think it makes sense to include these fields in a wide model like this, but open to suggestions
    ROW_NUMBER() OVER (PARTITION BY namespaces.ultimate_parent_namespace_id ORDER BY MIN(events.event_created_at) ASC)
      AS stage_order_adopted,-- Sequential order that this stage was adopted initially at the ultimate namespace level
    CONCAT(stage_order_adopted, ' ', stage_name)                                                                       AS stage_name_order -- This will allow me to create an ordered array
  FROM namespaces
  INNER JOIN mart_event_valid AS events -- Using this model to remove events triggered in Learn Gitlab projects (Growth use case)
    ON namespaces.ultimate_parent_namespace_id = events.dim_ultimate_parent_namespace_id
  WHERE is_smau = TRUE
    AND events.project_is_learn_gitlab != TRUE -- Filters out events caused by default Learn GitLab project creation.
    AND days_since_namespace_creation_at_event_date >= 0 -- Excluding events that occur before namespace creation
  GROUP BY 1, 2

),

stage_aggregation AS (

  SELECT
    ultimate_parent_namespace_id,
    MIN(IFF(stage_adoption.stage_name = 'plan', days_since_namespace_creation_at_first_event_date, NULL))
      AS days_since_namespace_creation_at_first_plan_event_date,
    MIN(IFF(stage_adoption.stage_name = 'secure', days_since_namespace_creation_at_first_event_date, NULL))
      AS days_since_namespace_creation_at_first_secure_event_date,
    MIN(IFF(stage_adoption.stage_name = 'create', days_since_namespace_creation_at_first_event_date, NULL))
      AS days_since_namespace_creation_at_first_create_event_date,
    MIN(IFF(stage_adoption.stage_name = 'verify', days_since_namespace_creation_at_first_event_date, NULL))
      AS days_since_namespace_creation_at_first_verify_event_date,
    MIN(IFF(stage_adoption.stage_name = 'release', days_since_namespace_creation_at_first_event_date, NULL))
      AS days_since_namespace_creation_at_first_release_event_date,
    MAX(IFF(stage_adoption.stage_name = 'plan', days_since_namespace_creation_at_latest_event_date, NULL))
      AS days_since_namespace_creation_at_latest_plan_event_date,
    MAX(IFF(stage_adoption.stage_name = 'secure', days_since_namespace_creation_at_latest_event_date, NULL))
      AS days_since_namespace_creation_at_latest_secure_event_date,
    MAX(IFF(stage_adoption.stage_name = 'create', days_since_namespace_creation_at_latest_event_date, NULL))
      AS days_since_namespace_creation_at_latest_create_event_date,
    MAX(IFF(stage_adoption.stage_name = 'verify', days_since_namespace_creation_at_latest_event_date, NULL))
      AS days_since_namespace_creation_at_latest_verify_event_date,
    MAX(IFF(stage_adoption.stage_name = 'release', days_since_namespace_creation_at_latest_event_date, NULL))
      AS days_since_namespace_creation_at_latest_release_event_date,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT stage_name_order) WITHIN GROUP (ORDER BY stage_name_order ASC), ' , ')
      AS stage_adoption_order,
    COUNT(DISTINCT stage_name)                                                                                AS total_stages_adopted --total stages adopted
  FROM stage_adoption
  GROUP BY 1

),

creator_attributes AS ( --ultimate namespace creator attributes

  SELECT DISTINCT
    namespaces.ultimate_parent_namespace_id,
    dim_marketing_contact_no_pii.sfdc_record_id,
    role               AS namespace_creator_role,
    jobs_to_be_done    AS namespace_creator_jtbd,
    is_first_order_person,
    is_defaulted_trial,
    sales_segment_name AS namespace_creator_sales_segment
  FROM namespaces
  INNER JOIN dim_user -- including all users with a membership to the ultimate parent regardless of creator status
    ON namespaces.creator_id = dim_user.dim_user_id
  LEFT JOIN dim_marketing_contact_no_pii -- Join on PQL information from PQL information
    ON namespaces.creator_id = dim_marketing_contact_no_pii.gitlab_dotcom_user_id
  LEFT JOIN mart_crm_person -- Get is_first_order_person, is_defaulted_trial, sales_segment
    ON dim_marketing_contact_no_pii.sfdc_record_id = mart_crm_person.sfdc_record_id

),

billable_members AS ( --billable members calculated to match user limit calculations

  SELECT
    namespaces.ultimate_parent_namespace_id, -- ultimate parent namespace
    COUNT(DISTINCT mships.user_id) AS billable_member_count
  FROM namespaces
  INNER JOIN gitlab_dotcom_memberships AS mships
    ON namespaces.ultimate_parent_namespace_id = mships.ultimate_parent_id
  WHERE mships.is_billable = TRUE
  GROUP BY 1

),

storage AS ( --current month storage snapshot

  SELECT DISTINCT
    namespaces.ultimate_parent_namespace_id,
    storage_gib
  FROM namespaces
  INNER JOIN fct_usage_storage AS storage
    ON namespaces.ultimate_parent_namespace_id = storage.ultimate_parent_namespace_id
  WHERE snapshot_month = DATE_TRUNC('month', GETDATE())::DATE --current month storage snapshot

),

team_activation_prep AS ( -- CTEs activation_events and second_billable_member are the building blocks for team activation - must union first

  SELECT DISTINCT
    ultimate_parent_namespace_id,
    event_activation_date  AS event_date,
    activation_event_array AS event_name
  FROM activation_events

  UNION ALL

  SELECT DISTINCT
    ultimate_parent_namespace_id,
    second_user_created_date AS event_date,
    'second user added'      AS event_name
  FROM second_billable_member

),

team_activation AS ( -- CTEs activation_events and second_billable_member are the building blocks for team activation

  SELECT
    ultimate_parent_namespace_id,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT event_name) WITHIN GROUP (ORDER BY event_name ASC), ' , ')
      AS activation_event_array,
    MIN(event_date)                                                                               AS team_activation_date
  FROM team_activation_prep
  GROUP BY 1

),

user_registration AS (

  SELECT
    user_id,
    TRIM(
      PARSE_JSON(onboarding_status)['registration_type'],
      '"'
    ) AS registration_type
  FROM
    {{ ref('gitlab_dotcom_user_details_dedupe_source') }}
  WHERE
    onboarding_status ILIKE '%registration_type%'

),

base AS (

  SELECT DISTINCT
    namespaces.ultimate_parent_namespace_id,
    namespaces.namespace_created_at,
    namespaces.namespace_created_date,
    DATE_TRUNC('month', namespaces.namespace_created_date)                                         AS namespace_created_month,
    DATE_TRUNC('week', namespaces.namespace_created_date)                                          AS namespace_created_week,
    namespaces.creator_id,
    creator_attributes.sfdc_record_id, -- Joined from dim_crm_person through dim_marketing_contact
    namespaces.namespace_type, -- Not limited to Group namespaces to facilitate broader analyses if needed
    namespaces.setup_for_company, -- User preferences field
    namespaces.visibility_level,
    namespaces.current_gitlab_plan_id,
    namespaces.current_gitlab_plan_title,
    namespaces.current_project_count,
    namespaces.days_since_namespace_creation,
    namespaces.handraise_pql_date,
    IFF(namespaces.handraise_pql_date IS NOT NULL, TRUE, FALSE)                                    AS is_hand_raise_pql,
    IFF(namespaces.is_namespace_created_within_2min_of_creator_invite_acceptance = 1, TRUE, FALSE)
      AS is_namespace_created_within_2min_of_creator_invite_acceptance, --consistent TRUE/FALSE formatting to match the rest of the resulting boolean values
    CASE
      WHEN namespaces.creator_invite_accepted_at IS NULL AND trials.first_trial_start_at IS NULL AND charges.first_paid_subscription_start_date IS NULL THEN 'free'
      WHEN TIMEDIFF('MINUTE', namespaces.creator_invite_accepted_at, namespaces.creator_created_at) BETWEEN 0 AND 2 THEN 'invite'
      WHEN TIMEDIFF('MINUTE', namespaces.creator_created_at, trials.first_trial_start_at) BETWEEN 0 AND 3 THEN 'trial'
      WHEN namespaces.creator_created_date = charges.first_paid_subscription_start_date THEN 'subscription'
      ELSE COALESCE(user_registration.registration_type, 'free')
    END                                                                                            AS registration_type, --https://gitlab.com/gitlab-data/product-analytics/-/issues/2180#note_2152839715
    trials.all_trials, -- array that includes all trials linked to the namespace
    trials.trial_start_date, -- trial 1
    trials.trial_type, -- trial 1
    trials.trial_type_name, -- trial 1
    trials.days_since_namespace_creation_at_trial, -- trial 1
    trials.trial_2_start_date,
    trials.trial_2_type_name,
    trials.days_since_namespace_creation_at_trial_2,
    trials.first_trial_type,
    trials.first_trial_type_name,
    trials.first_trial_start_date,
    trials.days_since_namespace_creation_at_first_trial,
    trials.latest_trial_type,
    trials.latest_trial_type_name,
    trials.latest_trial_start_date,
    trials.days_since_namespace_creation_at_latest_trial,
    charges.first_paid_subscription_start_date,
    charges.days_since_namespace_creation_at_first_paid_subscription,
    charges.first_paid_plan_name,
    COALESCE(charges.first_paid_plan_purchased_through_subscription_portal, FALSE)
      AS is_first_paid_plan_purchased_through_subscription_portal,
    DATEDIFF(DAYS, namespaces.namespace_created_date, team_activation.team_activation_date)
      AS days_since_namespace_creation_at_activation_event, -- all possible team activation events
    team_activation.activation_event_array, -- all possible team activation events within 14 days
    CASE WHEN DATEDIFF(DAYS, namespace_created_date, CURRENT_DATE()) < 59 THEN NULL -- If namespace is not at day 59 yet, then null
      WHEN d60_retention.ultimate_parent_namespace_id IS NOT NULL THEN TRUE
      ELSE FALSE
    END                                                                                            AS acitivity_between_d60_d90,
    COALESCE(valuable_signup.creator_is_valuable_signup, FALSE)                                    AS creator_is_valuable_signup,
    IFF(valuable_signup.ultimate_parent_namespace_id IS NOT NULL, TRUE, FALSE)
      AS namespace_contains_valuable_signup,
    second_billable_member.days_since_namespace_creation_at_2nd_user_add,
    IFF(team_activation.ultimate_parent_namespace_id IS NOT NULL, TRUE, FALSE)
      AS has_team_activation,
    creator_attributes.namespace_creator_role,
    creator_attributes.namespace_creator_jtbd,
    creator_attributes.is_first_order_person                                                       AS namespace_creator_is_first_order_person, -- SFDC is_first_order from the lead / contact record
    creator_attributes.namespace_creator_sales_segment,
    days_since_namespace_creation_at_first_plan_event_date,
    days_since_namespace_creation_at_first_secure_event_date,
    days_since_namespace_creation_at_first_create_event_date,
    days_since_namespace_creation_at_first_verify_event_date,
    days_since_namespace_creation_at_first_release_event_date,
    days_since_namespace_creation - days_since_namespace_creation_at_latest_plan_event_date
      AS days_since_latest_plan_event_date,
    days_since_namespace_creation - days_since_namespace_creation_at_latest_secure_event_date
      AS days_since_latest_secure_event_date,
    days_since_namespace_creation - days_since_namespace_creation_at_latest_create_event_date
      AS days_since_latest_create_event_date,
    days_since_namespace_creation - days_since_namespace_creation_at_latest_verify_event_date
      AS days_since_latest_verify_event_date,
    days_since_namespace_creation - days_since_namespace_creation_at_latest_release_event_date
      AS days_since_latest_release_event_date,
    stage_adoption_order,
    COALESCE(total_stages_adopted, 0)                                                              AS total_stages_adopted, --stages per ultimate namespace up to current
    billable_members.billable_member_count                                                         AS current_billable_member_count,
    storage.storage_gib                                                                            AS current_month_storage_gib,
    first_last_activity.min_event_date                                                             AS first_activity_date,
    first_last_activity.max_event_date                                                             AS latest_activity_date,
    team_activation.team_activation_date,
    namespaces.is_deleted,
    namespaces.is_deleted_updated_at
  FROM namespaces
  LEFT JOIN trials
    ON namespaces.ultimate_parent_namespace_id = trials.ultimate_parent_namespace_id
  LEFT JOIN charges
    ON namespaces.ultimate_parent_namespace_id = charges.ultimate_parent_namespace_id
  LEFT JOIN activation_events
    ON namespaces.ultimate_parent_namespace_id = activation_events.ultimate_parent_namespace_id
  LEFT JOIN d60_retention
    ON namespaces.ultimate_parent_namespace_id = d60_retention.ultimate_parent_namespace_id
  LEFT JOIN valuable_signup
    ON namespaces.ultimate_parent_namespace_id = valuable_signup.ultimate_parent_namespace_id
  LEFT JOIN second_billable_member
    ON namespaces.ultimate_parent_namespace_id = second_billable_member.ultimate_parent_namespace_id
  LEFT JOIN creator_attributes
    ON namespaces.ultimate_parent_namespace_id = creator_attributes.ultimate_parent_namespace_id
  LEFT JOIN stage_aggregation
    ON namespaces.ultimate_parent_namespace_id = stage_aggregation.ultimate_parent_namespace_id
  LEFT JOIN billable_members
    ON namespaces.ultimate_parent_namespace_id = billable_members.ultimate_parent_namespace_id
  LEFT JOIN storage
    ON namespaces.ultimate_parent_namespace_id = storage.ultimate_parent_namespace_id
  LEFT JOIN first_last_activity
    ON namespaces.ultimate_parent_namespace_id = first_last_activity.ultimate_parent_namespace_id
  LEFT JOIN team_activation
    ON namespaces.ultimate_parent_namespace_id = team_activation.ultimate_parent_namespace_id
  LEFT JOIN user_registration
    ON namespaces.creator_id = user_registration.user_id

)

{{ dbt_audit(
    cte_ref="base",
    created_by="@snalamaru",
    updated_by="@cbraza",
    created_date="2023-11-10",
    updated_date="2025-03-10"
) }}
