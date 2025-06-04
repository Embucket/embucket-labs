{{ config(
    materialized='table',
    tags=["mnpi_exception"],
    enabled=false
) }}

{{ simple_cte([
    ('dim_namespace','dim_namespace'),
    ('snowplow_sessions_all', 'snowplow_sessions_all'),
	('rpt_namespace_onboarding', 'rpt_namespace_onboarding'),
	('mart_crm_touchpoint', 'mart_crm_touchpoint'),
    ('dim_date', 'dim_date')
    ])
}},


-- Think of this as an event log
-- start with snowplow sessions, this will also be a part of the
-- final event output log
snowplow_sessions as (
    select
        user_snowplow_crossdomain_id,
        session_start,
        session_id,
        'snowplow_session' as event,
        first_page_url,
        first_page_url_query,
        first_gsc_namespace_id,
        first_top.ultimate_parent_namespace_id as first_ultimate_parent_namespace_id,
        last_gsc_namespace_id,
        last_top.ultimate_parent_namespace_id as last_ultimate_parent_namespace_id,
        --idea being that they created the namespace with the trial, also since we are saying
        -- they had to start with a UTM, its unlikley they started on the product
        COALESCE(last_ultimate_parent_namespace_id, first_ultimate_parent_namespace_id) as top_ultimate_parent_namespace_id,
        
        --putting the URL back together to use the native Snowflake parse_url() function
        concat(first_page_url_scheme, '://', first_page_url, '?', first_page_url_query) as first_page_full_url,
        
        -- UTMs
        PARSE_URL(first_page_full_url):parameters:utm_campaign::varchar      AS first_page_utm_campagin,
        PARSE_URL(first_page_full_url):parameters:utm_medium::varchar        AS first_page_utm_medium,
        PARSE_URL(first_page_full_url):parameters:utm_source::varchar        AS first_page_utm_source,
        PARSE_URL(first_page_full_url):parameters:utm_content::varchar       AS first_page_utm_content,
        PARSE_URL(first_page_full_url):parameters:utm_budget::varchar        AS first_page_utm_budget,
        PARSE_URL(first_page_full_url):parameters:utm_allptnr::varchar       AS first_page_utm_allptnr,
        PARSE_URL(first_page_full_url):parameters:utm_partnerid::varchar     AS first_page_utm_partnerid
    from
    snowplow_sessions_all
        -- get the top namespace
        left join dim_namespace first_top
            on snowplow_sessions_all.first_gsc_namespace_id = first_top.dim_namespace_id
        left join dim_namespace last_top
            on snowplow_sessions_all.last_gsc_namespace_id = last_top.dim_namespace_id
    where 1=1
     and snowplow_sessions_all.session_start > '2022-01-01'
     and contains(first_page_url_query, 'utm_')

), trial_and_free_union_namespaces as (
    -- This union creates a row for the trial, free and paid sign up. 
    -- This creates a crude event log that we can then use for attribution 
    -- in the next step

    -- free signups   
    select
        ultimate_parent_namespace_id,
        'signup_free' as signup_event,
        namespace_created_at as event_date,
        creator_id,
        sfdc_record_id,
        current_gitlab_plan_title,
        creator_is_valuable_signup,
        --need activation data
        days_since_namespace_creation_at_trial
    from
        rpt_namespace_onboarding 
    where 1=1
        and namespace_created_at > '2022-01-01'
        and trial_start_date is null

    union all
    
    -- trials first
    select
        ultimate_parent_namespace_id,
        'signup_trial' as signup_event,
        trial_start_date as event_date,
        creator_id,
        sfdc_record_id,
        current_gitlab_plan_title,
        creator_is_valuable_signup,
        --need activation data
        days_since_namespace_creation_at_trial
    from
        rpt_namespace_onboarding 
    where 1=1
        and namespace_created_at > '2022-01-01'
        and trial_start_date is not null
        and trial_start_date >= namespace_created_at 

    union all

    -- paid signups
    select
        ultimate_parent_namespace_id,
        'signup_paid' as signup_event,
        first_paid_subscription_start_date as event_date,
        creator_id,
        sfdc_record_id,
        current_gitlab_plan_title,
        creator_is_valuable_signup,
        --need activation data
        days_since_namespace_creation_at_trial
    from
        rpt_namespace_onboarding 
    where 1=1
        and namespace_created_at > '2022-01-01'
        and first_paid_subscription_start_date is not null
        and first_paid_subscription_start_date >= namespace_created_at
        
), last_touch_attribution_session_to_event as (
    -- You've found the magic, here we use last touch attribution to find the 
    -- the most recent session with UTMs and assign it a matching paid, trial or free namespace
    -- The output of this CTE will become a row in the final event log
    
    -- These statements will only output a row if they find a match, and because
    -- we started with sessions with UTMs (discounting date range) we can assume marketing should
    -- take some credit for the match.
    -- hours_to_signup_event - mesaures the time from the start of the session to signing up.
    -- This is set at a high value so we can trim it down in Tableau

    -- why three seperate statements?
    -- A user could go from free -> trial -> paid in the same seession 
    -- so a single session could be responsible for all three
    -- When presenting these metrics, will be imoortant to state they are subsets of eachother
    -- For example - if you have 30 free, 20 trials, and 10 paid on the same UTM then those
    -- should be viewed as subsets of eachother
    
    -- free
    select
        trial_and_free_union_namespaces.*,
        snowplow_sessions.*,
        'attributed_free_signup' as attribution_event,
        datediff('hour', event_date, session_start) as hours_to_signup_event,
        row_number() over (partition by ultimate_parent_namespace_id order by SESSION_START DESC) as session_row_number
    from
        snowplow_sessions
        join trial_and_free_union_namespaces 
            on snowplow_sessions.top_ultimate_parent_namespace_id = trial_and_free_union_namespaces.ultimate_parent_namespace_id
               and trial_and_free_union_namespaces.event_date::date >= session_start::date
               and trial_and_free_union_namespaces.signup_event = 'signup_free'
        where 1=1
            and hours_to_signup_event <= (180*24) -- 180 days
    qualify session_row_number = 1

    union all
    
    -- trial
    select
        trial_and_free_union_namespaces.*,
        snowplow_sessions.*,
        'attributed_trial_signup' as attribution_event,
        datediff('hour', event_date, session_start) as hours_to_signup_event,
        row_number() over (partition by ultimate_parent_namespace_id order by SESSION_START DESC) as session_row_number
    from
        snowplow_sessions
        join trial_and_free_union_namespaces 
            on snowplow_sessions.top_ultimate_parent_namespace_id = trial_and_free_union_namespaces.ultimate_parent_namespace_id
               and trial_and_free_union_namespaces.event_date::date >= session_start::date
               and trial_and_free_union_namespaces.signup_event = 'signup_trial'
        where 1=1
            and hours_to_signup_event <= (180*24) -- 180 days
    qualify session_row_number = 1
    
    union all
    
    -- paid
    select
        trial_and_free_union_namespaces.*,
        snowplow_sessions.*,
        'attributed_paid_signup' as attribution_event,
        datediff('hour', event_date, session_start) as hours_to_signup_event,
        row_number() over (partition by ultimate_parent_namespace_id order by SESSION_START DESC) as session_row_number
    from
        snowplow_sessions
        join trial_and_free_union_namespaces 
            on snowplow_sessions.top_ultimate_parent_namespace_id = trial_and_free_union_namespaces.ultimate_parent_namespace_id
               and trial_and_free_union_namespaces.event_date::date >= session_start::date
               and trial_and_free_union_namespaces.signup_event = 'signup_paid'
        where 1=1
            and hours_to_signup_event <= (180*24) -- 180 days
    qualify session_row_number = 1

), form_submits as (
    -- Add in form submits by UTM as non-attribtion events
    -- This will be usefult to give context to each campaign
    -- "This UTM created x many trials and y many form submits"
    -- currently using bizible, but this should be switched for Marketo
    select
        bizible_touchpoint_date as event_date,
       'form_submit' as event,

        null as session_id,
        null as user_snowplow_crossdomain_id,
        bizible_landing_page as first_page_url,
        null as top_ultimate_parent_namespace_id,
        PARSE_URL(bizible_landing_page_raw):parameters:utm_campaign::varchar      AS first_page_utm_campagin,
        PARSE_URL(bizible_landing_page_raw):parameters:utm_medium::varchar        AS first_page_utm_medium,
        PARSE_URL(bizible_landing_page_raw):parameters:utm_source::varchar        AS first_page_utm_source,
        PARSE_URL(bizible_landing_page_raw):parameters:utm_content::varchar       AS first_page_utm_content,
        PARSE_URL(bizible_landing_page_raw):parameters:utm_budget::varchar        AS first_page_utm_budget,
        PARSE_URL(bizible_landing_page_raw):parameters:utm_allptnr::varchar       AS first_page_utm_allptnr,
        PARSE_URL(bizible_landing_page_raw):parameters:utm_partnerid::varchar     AS first_page_utm_partnerid,

        -- trial data
        null as creator_id,
        null as sfdc_record_id,
        null as current_gitlab_plan_title,
        null as creator_is_valuable_signup,
        --need activation data
        
        -- attribtion data
        null as days_since_namespace_creation_at_trial
    from
    mart_crm_touchpoint
    where 1=1
    and bizible_touchpoint_date > '2022-01-01'
    and contains(bizible_landing_page_raw, 'utm')

), build_final_event_log as (
-- put it all together

    -- form submits
    -- format is done in the single CTE
    select
        *
    from
    form_submits
    
    union all

    -- get all the snowplow sessions
    select
        session_start as event_date,
        event,
        session_id,
        user_snowplow_crossdomain_id,
        first_page_url,
        top_ultimate_parent_namespace_id,
        first_page_utm_campagin,
        first_page_utm_medium,
        first_page_utm_source,
        first_page_utm_content,
        first_page_utm_budget,
        first_page_utm_allptnr,
        first_page_utm_partnerid,

        -- trial data
        null as creator_id,
        null as sfdc_record_id,
        null as current_gitlab_plan_title,
        null as creator_is_valuable_signup,
        --need activation data
        
        -- attribtion data
        null as days_since_namespace_creation_at_trial
    from
    snowplow_sessions

    union all

    -- get events from the attribution cte
    select
        event_date,
        attribution_event as event,
    
        -- session data
        session_id,
        user_snowplow_crossdomain_id,
        first_page_url,
        top_ultimate_parent_namespace_id,
        first_page_utm_campagin,
        first_page_utm_medium,
        first_page_utm_source,
        first_page_utm_content,
        first_page_utm_budget,
        first_page_utm_allptnr,
        first_page_utm_partnerid,

        -- trail data
        creator_id,
        sfdc_record_id,
        current_gitlab_plan_title,
        creator_is_valuable_signup,
        --need activation data
        
        -- attribtion data
        days_since_namespace_creation_at_trial
        
    from
    last_touch_attribution_session_to_event

    -- I'm choosing not add the trial_and_free_union_namespaces cte in.

)

{{ dbt_audit(
    cte_ref="build_final_event_log",
    created_by="@degan",
    updated_by="@degan",
    created_date="2023-06-30",
    updated_date="2023-07-26"
) }}
