{{ config(
    tags=["mnpi_exception"],
    materialized='table'
) }}

{{ simple_cte([
    ('wk_marketo_activity_fill_out_form','wk_marketo_activity_fill_out_form'),
    ('mart_marketing_contact_no_pii','mart_marketing_contact_no_pii'),
    ('mart_crm_person','mart_crm_person'), 
    ('mart_crm_touchpoint','mart_crm_touchpoint')
]) }}

, marketo_form_fill AS (
  
  SELECT DISTINCT
    wk_marketo_activity_fill_out_form.lead_id::VARCHAR AS marketo_lead_id,
    wk_marketo_activity_fill_out_form.activity_date::DATE AS activity_date,
    wk_marketo_activity_fill_out_form.primary_attribute_value AS marketo_form_name,
    CASE 
      WHEN referrer_url LIKE '%free-trial/index%' THEN 'https://about.gitlab.com/free-trial/.html'
      ELSE referrer_url
    END AS trial_url,
    SPLIT_PART(trial_url,'?',1) AS raw_url,
    SPLIT_PART(raw_url,'.html',1) AS raw_url2,
    SPLIT_PART(raw_url2,'.com',2) AS form_url,
    mart_marketing_contact_no_pii.sfdc_record_id
  FROM wk_marketo_activity_fill_out_form
  LEFT JOIN mart_marketing_contact_no_pii
    ON wk_marketo_activity_fill_out_form.lead_id=mart_marketing_contact_no_pii.marketo_lead_id
  LEFT JOIN mart_crm_person
    ON wk_marketo_activity_fill_out_form.lead_id=mart_crm_person.marketo_lead_id
  WHERE LOWER(wk_marketo_activity_fill_out_form.primary_attribute_value) NOT LIKE '%unsubscribe%'
  AND wk_marketo_activity_fill_out_form.primary_attribute_value != 'PathFactory Webhook'
  AND wk_marketo_activity_fill_out_form.primary_attribute_value NOT LIKE '%EmailSubscription%'
  AND form_url IS NOT null
  AND activity_date <= CURRENT_DATE - 2
  AND activity_date >= CURRENT_DATE - 3
  AND mart_marketing_contact_no_pii.sfdc_record_id IS NOT NULL
  
  
), mart_crm_touchpoint_base AS (
  
  SELECT 
    mart_crm_touchpoint.marketo_lead_id::VARCHAR AS marketo_lead_id,
    dim_crm_touchpoint_id,
    bizible_touchpoint_date,
    CASE
      WHEN bizible_form_url_raw IS NULL 
        THEN bizible_landing_page_raw
      WHEN bizible_landing_page_raw IS NULL
        THEN bizible_referrer_page_raw
      WHEN bizible_form_url_raw = 'https://about.gitlab.com/'
        THEN bizible_landing_page_raw
      WHEN bizible_form_url_raw LIKE '%free-trial/index%'
        THEN 'https://about.gitlab.com/free-trial/.html'
      ELSE bizible_form_url_raw
    END AS bizible_form_url1,
    SPLIT_PART(concat('/', PARSE_URL(bizible_form_url1):path::VARCHAR), 'https:/', 1) AS bizible_form_url2,
    SPLIT_PART(bizible_form_url2, '.html', 1) AS bizible_form_url,
    mart_crm_touchpoint.dim_crm_person_id,
    mart_crm_touchpoint.sfdc_record_id
  FROM mart_crm_touchpoint
  LEFT JOIN mart_crm_person
    ON mart_crm_touchpoint.dim_crm_person_id=mart_crm_person.dim_crm_person_id
  WHERE bizible_touchpoint_type = 'Web Form'
  
), intermediate AS (

  SELECT
    marketo_form_fill.marketo_lead_id,
    marketo_form_fill.sfdc_record_id,
    activity_date,
    marketo_form_fill.form_url,
    marketo_form_fill.trial_url AS full_url,
    SPLIT_PART(full_url, '&mkt_tok', 1) AS final_url,
    marketo_form_name
  FROM marketo_form_fill
  FULL JOIN mart_crm_touchpoint_base
    ON 
  mart_crm_touchpoint_base.sfdc_record_id=marketo_form_fill.sfdc_record_id
    AND
  mart_crm_touchpoint_base.bizible_form_url=marketo_form_fill.form_url
    AND (mart_crm_touchpoint_base.bizible_touchpoint_date+INTERVAL '1 Hour'=marketo_form_fill.activity_date+INTERVAL '1 Hour'
         OR mart_crm_touchpoint_base.bizible_touchpoint_date+INTERVAL '1 Hour'=marketo_form_fill.activity_date-INTERVAL '1 Hour'
         OR mart_crm_touchpoint_base.bizible_touchpoint_date-INTERVAL '1 Hour'=marketo_form_fill.activity_date-INTERVAL '1 Hour'
         OR mart_crm_touchpoint_base.bizible_touchpoint_date-INTERVAL '1 Hour'=marketo_form_fill.activity_date+INTERVAL '1 Hour'
        OR mart_crm_touchpoint_base.bizible_touchpoint_date=marketo_form_fill.activity_date)
  WHERE marketo_form_fill.activity_date IS NOT NULL
    AND dim_crm_touchpoint_id IS NULL

), final AS (
  
  SELECT
    marketo_lead_id,
    sfdc_record_id,
    activity_date::DATE AS activity_date,
    marketo_form_name,
    form_url,
    final_url,
    {{ dbt_utils.get_url_parameter(field='final_url', url_parameter='utm_campaign') }} AS utm_campaign,
    {{ dbt_utils.get_url_parameter(field='final_url', url_parameter='utm_channel') }} AS utm_channel,
    {{ dbt_utils.get_url_parameter(field='final_url', url_parameter='utm_medium') }} AS utm_medium,
    {{ dbt_utils.get_url_parameter(field='final_url', url_parameter='utm_source') }} AS utm_source
  FROM intermediate

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-09-12",
    updated_date="2023-12-19",
  ) }}