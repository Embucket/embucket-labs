{{ config({
    "materialized": "incremental",
    "unique_key": "user_id"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_ops', 'users') }}
  {% if is_incremental() %}
    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}

),

renamed AS (

  SELECT
    id::NUMBER                                            AS user_id,
    remember_created_at::TIMESTAMP                        AS remember_created_at,
    sign_in_count::NUMBER                                 AS sign_in_count,
    current_sign_in_at::TIMESTAMP                         AS current_sign_in_at,
    last_sign_in_at::TIMESTAMP                            AS last_sign_in_at,
    -- current_sign_in_ip   // hidden for privacy
    -- last_sign_in_ip   // hidden for privacy
    created_at::TIMESTAMP                                 AS created_at,
    updated_at::TIMESTAMP                                 AS updated_at,
    admin::BOOLEAN                                        AS is_admin,
    projects_limit::NUMBER                                AS projects_limit,
    failed_attempts::NUMBER                               AS failed_attempts,
    locked_at::TIMESTAMP                                  AS locked_at,
    IFF(LOWER(locked_at) = 'nan', FALSE, TRUE)            AS user_locked,
    can_create_group::BOOLEAN                             AS has_create_group_permissions,
    can_create_team::BOOLEAN                              AS has_create_team_permissions,
    state,
    color_scheme_id::NUMBER                               AS color_scheme_id,
    password_expires_at::TIMESTAMP                        AS password_expires_at,
    created_by_id::NUMBER                                 AS created_by_id,
    last_credential_check_at::TIMESTAMP                   AS last_credential_check_at,
    IFF(LOWER(avatar) = 'nan', FALSE, TRUE)               AS has_avatar,
    confirmed_at::TIMESTAMP                               AS confirmed_at,
    confirmation_sent_at::TIMESTAMP                       AS confirmation_sent_at,
    -- unconfirmed_email // hidden for privacy
    hide_no_ssh_key::BOOLEAN                              AS has_hide_no_ssh_key_enabled,
    admin_email_unsubscribed_at::TIMESTAMP                AS admin_email_unsubscribed_at,
    notification_email::VARCHAR                           AS notification_email,
    hide_no_password::BOOLEAN                             AS has_hide_no_password_enabled,
    password_automatically_set::BOOLEAN                   AS is_password_automatically_set,
    IFF(LOWER(location) = 'nan', NULL, location)          AS location,
    public_email::VARCHAR                                 AS public_email,
    dashboard::NUMBER                                     AS dashboard,
    project_view::NUMBER                                  AS project_view,
    consumed_timestep::NUMBER                             AS consumed_timestep,
    layout::NUMBER                                        AS layout,
    hide_project_limit::BOOLEAN                           AS has_hide_project_limit_enabled,
    -- note // hidden for privacy
    otp_grace_period_started_at::TIMESTAMP                AS otp_grace_period_started_at,
    external::BOOLEAN                                     AS is_external_user,
    organization,
    auditor::BOOLEAN                                      AS is_auditor,
    require_two_factor_authentication_from_group::BOOLEAN AS does_require_two_factor_authentication_from_group,
    two_factor_grace_period::NUMBER                       AS two_factor_grace_period,
    last_activity_on::TIMESTAMP                           AS last_activity_on,
    notified_of_own_activity::BOOLEAN                     AS is_notified_of_own_activity,
    NULLIF(preferred_language, 'nan')::VARCHAR            AS preferred_language,
    theme_id::NUMBER                                      AS theme_id,
    accepted_term_id::NUMBER                              AS accepted_term_id,
    private_profile::BOOLEAN                              AS is_private_profile,
    roadmap_layout::NUMBER                                AS roadmap_layout,
    include_private_contributions::BOOLEAN                AS include_private_contributions,
    group_view::NUMBER                                    AS group_view,
    managing_group_id::NUMBER                             AS managing_group_id,
    -- bot_type::NUMBER  // removed from prod
    source.role::NUMBER                                   AS role_id,
    {{ user_role_mapping(user_role='source.role') }}::VARCHAR                                          AS role,
    username::VARCHAR                                     AS user_name,
    first_name::VARCHAR                                   AS first_name,
    last_name::VARCHAR                                    AS last_name,
    name::VARCHAR                                         AS users_name,
    user_type::NUMBER                                     AS user_type

  FROM source
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) = 1

)

SELECT *
FROM renamed
ORDER BY updated_at
