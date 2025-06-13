{{ config({
    "materialized": "incremental",
    "unique_key": "fct_behavior_website_page_view_sk",
    "tags": ["product","mnpi_exception"]
    })
}}

{{ simple_cte([
    ('fct_behavior_website_page_view','fct_behavior_website_page_view'),
    ('dim_behavior_website_page', 'dim_behavior_website_page'),
    ])
}},

website_page_views AS (

  SELECT
    fct.fct_behavior_website_page_view_sk,
    fct.dim_behavior_website_page_sk,
    fct.dim_behavior_referrer_page_sk,
    fct.dim_namespace_id,
    fct.dim_project_id,
    fct.page_view_start_at,
    fct.page_view_end_at,
    fct.behavior_at,
    fct.session_id,
    fct.event_id,
    fct.user_snowplow_domain_id,
    fct.gsc_extra,
    fct.gsc_google_analytics_client_id,
    fct.gsc_plan,
    fct.gsc_pseudonymized_user_id,
    fct.gsc_source,
    fct.page_url_path,
    fct.referer_url_path,
    fct.event_name,
    fct.sf_formid,
    fct.engaged_seconds,
    fct.page_load_time_in_ms,
    fct.page_view_index,
    fct.page_view_in_session_index,
    dim.page_url_host_path,
    dim.app_id,
    dim.page_url_host,
    dim.page_url_query,
    dim.clean_url_path,
    dim.page_url_scheme,
    dim.page_group,
    dim.page_type,
    dim.page_sub_type,
    dim.referrer_medium,
    dim.url_namespace_id,
    dim.url_project_id,
    dim.url_path_category,
    dim.is_url_interacting_with_security
  FROM fct_behavior_website_page_view AS fct
  INNER JOIN dim_behavior_website_page AS dim
    ON fct.dim_behavior_website_page_sk = dim.dim_behavior_website_page_sk
  WHERE dim.clean_url_path IN ( -- all URLs involved in registration flow https://docs.google.com/presentation/d/1bb4vFu6Nd11itFJxNJO4eim-xnFbJw3pHCuknNPXk_g/edit?usp=sharing
    'trial_registrations',
    'trial_registrations/new',
    'trials/new',
    'users/sign_up',
    'users/identity_verification',
    'users/identity_verification/success',
    'users/sign_up/welcome',
    'users/sign_up/company',
    'users/sign_up/company/new',
    'users/sign_up/groups/new',
    'namespace/project/learn_gitlab',
    'namespace/project/learn_gitlab/onboarding'
    )
    AND fct.event_name = 'page_view'
    AND dim.app_id IN ('gitlab', 'gitlab_customers') -- production app_id
    AND fct.behavior_at::DATE >= DATEADD(DAY, -190, CURRENT_DATE::DATE)
  {% if is_incremental() %}

    AND  fct.behavior_at > (SELECT MAX(behavior_at) FROM {{ this }})

  {% endif %}

)

{{ dbt_audit(
    cte_ref="website_page_views",
    created_by="@eneuberger",
    updated_by="@mdrussell",
    created_date="2023-07-12",
    updated_date="2023-07-17"
) }}
