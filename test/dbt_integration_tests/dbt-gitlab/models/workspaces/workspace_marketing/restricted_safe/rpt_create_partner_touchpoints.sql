{{ config(materialized='table') }}

{{ simple_cte([
    ('mart_crm_event','mart_crm_event'),
    ('mart_crm_task','mart_crm_task'),
    ('mart_crm_account', 'mart_crm_account'),
    ('sfdc_campaign_member', 'sfdc_campaign_member'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('mart_crm_person', 'mart_crm_person'),
    ('dim_date', 'dim_date') 
]) }},

activities as (
  SELECT
    mart_crm_event.event_id AS activity_id,
    mart_crm_event.dim_crm_user_id,
    mart_crm_event.dim_crm_opportunity_id,
    mart_crm_event.dim_crm_account_id,
    mart_crm_event.sfdc_record_id,
    mart_crm_event.dim_crm_person_id,
    mart_crm_event.partner_marketing_task_subject,
    mart_crm_event.event_date       AS activity_date,
    dim_date.day_of_fiscal_quarter  as activity_day_of_fiscal_quarter,
    dim_date.fiscal_quarter_name_fy as activity_fiscal_quarter_name,
    'Event'                         AS activity_type,
    mart_crm_event.event_type       AS activity_subtype
  FROM mart_crm_event
    LEFT JOIN dim_date 
    ON mart_crm_event.event_date = dim_date.date_day
  UNION ALL
  SELECT
    mart_crm_task.task_id AS activity_id,
    mart_crm_task.dim_crm_user_id,
    mart_crm_task.dim_crm_opportunity_id,
    mart_crm_task.dim_crm_account_id,
    mart_crm_task.sfdc_record_id,
    mart_crm_task.dim_crm_person_id,
    mart_crm_task.partner_marketing_task_subject,
    mart_crm_task.task_completed_date AS activity_date,
    dim_date.day_of_fiscal_quarter    as activity_day_of_fiscal_quarter,
    dim_date.fiscal_quarter_name_fy   as activity_fiscal_quarter_name,
    mart_crm_task.task_type           AS activity_type,
    mart_crm_task.task_subtype        AS activity_subtype
  FROM mart_crm_task
  LEFT JOIN dim_date 
    ON mart_crm_task.task_completed_date = dim_date.date_day

), get_accounts_from_tasks as (
    select
        activities.dim_crm_account_id,
        mart_crm_account.crm_account_type,
        mart_crm_account.crm_account_name,
        mart_crm_account.parent_crm_account_name
    from
    activities
        left join mart_crm_account
          on activities.dim_crm_account_id = mart_crm_account.dim_crm_account_id
    where partner_marketing_task_subject IS NOT NULL
    group by all

), get_members_of_partner_camapgin as (
    select
    campaign_member_created_date,
    lead_or_contact_id as person_sfdc_record_id,
    -- dim_crm_person_id,
    mart_crm_person.dim_crm_account_id,
    mart_crm_person.marketo_lead_id,
    crm_account_type
    from
    sfdc_campaign_member
        join mart_crm_person
            on sfdc_campaign_member.lead_or_contact_id = mart_crm_person.sfdc_record_id
        left join mart_crm_account
            on mart_crm_person.dim_crm_account_id = mart_crm_account.dim_crm_account_id
            and (crm_account_type != 'Partner')
    where 
    campaign_id = '7014M000001dosSQAQ'
    and lead_source in ('Partner Qualified Lead','Channel Qualified Lead')
    
), find_deal_reg_of_partners as (
    select
        opp.dim_crm_account_id,
        opp.partner_account,
        opp.partner_account_name,
        opp.crm_account_name
    from
    mart_crm_opportunity opp
        join get_accounts_from_tasks
            on get_accounts_from_tasks.dim_crm_account_id = opp.partner_account

), match_to_campaign as (
    select
    get_members_of_partner_camapgin.campaign_member_created_date,
    get_members_of_partner_camapgin.person_sfdc_record_id,
    get_members_of_partner_camapgin.marketo_lead_id,
    get_members_of_partner_camapgin.dim_crm_account_id,
    get_members_of_partner_camapgin.crm_account_type,
    find_deal_reg_of_partners.partner_account,
    find_deal_reg_of_partners.partner_account_name,
    find_deal_reg_of_partners.crm_account_name,
    coalesce(partner_account_name, '')           as utm_source,
    'partner'                                    as utm_medium,
    concat('https://injected-touchpoint.gitlab.com?utm_source=', lower(utm_source), '&utm_medium=partner') as touchpoint_url
    from
    get_members_of_partner_camapgin
        join find_deal_reg_of_partners
        on get_members_of_partner_camapgin.dim_crm_account_id = find_deal_reg_of_partners.dim_crm_account_id
), final as (
    select distinct
    *
    from
    match_to_campaign
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@degan",
    updated_by="@michellecooper",
    created_date="2024-04-23",
    updated_date="2024-05-17",
) }}

