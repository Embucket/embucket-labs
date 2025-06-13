{{ config({
    "tags": ["product"],
    "post-hook": "{{ missing_member_column(primary_key = 'dim_user_sk', not_null_test_cols = ['dim_user_id', 'user_id']) }}",
    "snowflake_warehouse": generate_warehouse_name('XL')
}) }}

WITH prep_user AS (

  SELECT
    --surrogate_key
    dim_user_sk,

    --natural_key
    user_id,

    --legacy natural_key to be deprecated during change management plan
    dim_user_id,

    --Other attributes
    it_job_title_hierarchy,
    user_name,
    users_name,
    public_email,
    remember_created_at,
    sign_in_count,
    current_sign_in_at,
    last_sign_in_at,
    created_at,
    updated_at,
    notification_email_domain,
    notification_email_domain_classification,
    email_domain,
    email_domain_classification,
    IFF(email_domain_classification IS NULL, TRUE, FALSE) AS is_valuable_signup,
    public_email_domain,
    public_email_domain_classification,
    commit_email_domain,
    commit_email_domain_classification,
    identity_provider,
    role,
    last_activity_date,
    last_sign_in_date,
    setup_for_company,
    jobs_to_be_done,
    for_business_use,
    employee_count,
    country,
    state,
    timezone,
    early_access_program_participant,
    preferred_language,
    user_locked,
    job_title,
    registration_objective,
    user_type_id,
    user_type,
    locked_at,
    created_by_id,
    account_age,
    account_age_cohort,
    days_from_account_creation_to_last_activity,
    user_state,

    -- Highest paid data background - https://gitlab.com/gitlab-data/analytics/-/merge_requests/9987#note_1929662333
    highest_paid_subscription_plan_id,

    -- Flags
    is_admin,
    is_blocked_user,
    is_bot,
    is_auditor,
    is_external_user,
    is_notified_of_own_activity,
    is_private_profile,
    has_create_group_permissions,
    has_create_team_permissions,
    has_hide_no_password_enabled,
    has_hide_no_ssh_key_enabled,
    has_hide_project_limit_enabled,
    include_private_contributions
  FROM {{ ref('prep_user') }}

)

{{ dbt_audit(
    cte_ref="prep_user",
    created_by="@mpeychet_",
    updated_by="@michellecooper",
    created_date="2021-06-28",
    updated_date="2024-07-31"
) }}
