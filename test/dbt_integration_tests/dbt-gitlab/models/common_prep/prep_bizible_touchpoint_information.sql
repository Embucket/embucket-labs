{{ config(
    materialized="table",
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('sfdc_lead_source','sfdc_lead_source'),
    ('sfdc_contact_source','sfdc_contact_source'),
    ('sfdc_bizible_person_source','sfdc_bizible_person_source'),
    ('sfdc_bizible_touchpoint_source','sfdc_bizible_touchpoint_source')
]) }}

, prep_lead AS (

    SELECT
        lead_id AS sfdc_record_id,
        converted_contact_id,
        marketo_qualified_lead_datetime,
        mql_datetime_inferred
    FROM sfdc_lead_source
    WHERE is_deleted = 'FALSE'

), prep_contact AS (

    SELECT
        contact_id AS sfdc_record_id,
        marketo_qualified_lead_datetime,
        mql_datetime_inferred
    FROM sfdc_contact_source
    WHERE is_deleted = 'FALSE'

), prep_bizible AS (

    SELECT 
        sfdc_bizible_touchpoint_source.*,
        sfdc_bizible_person_source.bizible_contact_id,
        sfdc_bizible_person_source.bizible_lead_id
    FROM sfdc_bizible_touchpoint_source
    LEFT JOIN sfdc_bizible_person_source
        ON sfdc_bizible_touchpoint_source.bizible_person_id = sfdc_bizible_person_source.person_id
    WHERE sfdc_bizible_person_source.is_deleted = 'FALSE' 
        AND sfdc_bizible_touchpoint_source.is_deleted = 'FALSE' 

), prep_person AS (

    SELECT
        sfdc_record_id,
        bizible_person_id
    FROM prep_lead
    LEFT JOIN prep_bizible
        ON prep_lead.sfdc_record_id=prep_bizible.bizible_lead_id
    UNION ALL
    SELECT
        sfdc_record_id,
        bizible_person_id
    FROM prep_contact
    LEFT JOIN prep_bizible
        ON prep_contact.sfdc_record_id=prep_bizible.bizible_contact_id

), marketing_qualified_leads AS (
    
    SELECT
        {{ dbt_utils.generate_surrogate_key(['COALESCE(converted_contact_id, sfdc_record_id)','marketo_qualified_lead_datetime::timestamp']) }} AS mql_event_id,
        sfdc_record_id,
        MAX(marketo_qualified_lead_datetime)::timestamp AS mql_date_latest
    FROM prep_lead
    WHERE marketo_qualified_lead_datetime IS NOT NULL
        OR mql_datetime_inferred IS NOT NULL
    GROUP BY 1,2


), marketing_qualified_contacts AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['sfdc_record_id','marketo_qualified_lead_datetime::timestamp']) }} AS mql_event_id,
        sfdc_record_id,
        MAX(marketo_qualified_lead_datetime)::timestamp AS mql_date_latest
    FROM prep_contact
    WHERE marketo_qualified_lead_datetime IS NOT NULL
        OR mql_datetime_inferred IS NOT NULL
    GROUP BY 1,2
    HAVING sfdc_record_id NOT IN (
                         SELECT sfdc_record_id
                         FROM marketing_qualified_leads
                         )

), mql_person_prep AS (

    SELECT
        sfdc_record_id,
        mql_date_latest
    FROM marketing_qualified_leads
    UNION ALL
    SELECT
        sfdc_record_id,
        mql_date_latest
    FROM marketing_qualified_contacts
    
), bizible_mql_touchpoint_information_base AS (

    SELECT DISTINCT
        prep_person.sfdc_record_id,
        prep_bizible.touchpoint_id,
        prep_bizible.bizible_touchpoint_date,
        prep_bizible.bizible_form_url,
        prep_bizible.campaign_id AS sfdc_campaign_id,
        prep_bizible.bizible_ad_campaign_name,
        prep_bizible.bizible_marketing_channel,
        prep_bizible.bizible_marketing_channel_path,
        ROW_NUMBER () OVER (PARTITION BY prep_person.sfdc_record_id ORDER BY prep_bizible.bizible_touchpoint_date DESC) AS touchpoint_order_by_person
    FROM prep_person
    LEFT JOIN mql_person_prep
        ON prep_person.sfdc_record_id=mql_person_prep.sfdc_record_id
    LEFT JOIN prep_bizible
        ON prep_person.bizible_person_id = prep_bizible.bizible_person_id
    WHERE prep_bizible.touchpoint_id IS NOT null
        AND mql_person_prep.mql_date_latest IS NOT null
        AND prep_bizible.bizible_touchpoint_date::DATE <= mql_person_prep.mql_date_latest::DATE
    ORDER BY prep_bizible.bizible_touchpoint_date DESC

), bizible_mql_touchpoint_information_final AS (

  SELECT
      sfdc_record_id AS biz_mql_person_id,
      touchpoint_id AS bizible_mql_touchpoint_id,
      bizible_touchpoint_date AS bizible_mql_touchpoint_date,
      bizible_form_url AS bizible_mql_form_url,
      sfdc_campaign_id AS bizible_mql_sfdc_campaign_id,
      bizible_ad_campaign_name AS bizible_mql_ad_campaign_name,
      bizible_marketing_channel AS bizible_mql_marketing_channel,
      bizible_marketing_channel_path AS bizible_mql_marketing_channel_path
  FROM bizible_mql_touchpoint_information_base
  WHERE touchpoint_order_by_person = 1

), bizible_most_recent_touchpoint_information_base AS (

    SELECT DISTINCT
        prep_person.sfdc_record_id,
        prep_bizible.touchpoint_id,
        prep_bizible.bizible_touchpoint_date,
        prep_bizible.bizible_form_url,
        prep_bizible.campaign_id AS sfdc_campaign_id,
        prep_bizible.bizible_ad_campaign_name,
        prep_bizible.bizible_marketing_channel,
        prep_bizible.bizible_marketing_channel_path,
        ROW_NUMBER () OVER (PARTITION BY prep_person.sfdc_record_id ORDER BY prep_bizible.bizible_touchpoint_date DESC) AS touchpoint_order_by_person
    FROM prep_person
    LEFT JOIN prep_bizible
        ON prep_person.bizible_person_id = prep_bizible.bizible_person_id
    WHERE prep_bizible.touchpoint_id IS NOT null

), bizible_most_recent_touchpoint_information_final AS (

  SELECT
      sfdc_record_id AS biz_most_recent_person_id,
      touchpoint_id AS bizible_most_recent_touchpoint_id,
      bizible_touchpoint_date AS bizible_most_recent_touchpoint_date,
      bizible_form_url AS bizible_most_recent_form_url,
      sfdc_campaign_id AS bizible_most_recent_sfdc_campaign_id,
      bizible_ad_campaign_name AS bizible_most_recent_ad_campaign_name,
      bizible_marketing_channel AS bizible_most_recent_marketing_channel,
      bizible_marketing_channel_path AS bizible_most_recent_marketing_channel_path
  FROM bizible_most_recent_touchpoint_information_base
  WHERE touchpoint_order_by_person = 1

), final AS (

    SELECT 
        bizible_most_recent_touchpoint_information_final.biz_most_recent_person_id AS sfdc_record_id,
        bizible_mql_touchpoint_information_final.bizible_mql_touchpoint_id,
        bizible_mql_touchpoint_information_final.bizible_mql_touchpoint_date,
        bizible_mql_touchpoint_information_final.bizible_mql_form_url,
        bizible_mql_touchpoint_information_final.bizible_mql_sfdc_campaign_id,
        bizible_mql_touchpoint_information_final.bizible_mql_ad_campaign_name,
        bizible_mql_touchpoint_information_final.bizible_mql_marketing_channel,
        bizible_mql_touchpoint_information_final.bizible_mql_marketing_channel_path,
        bizible_most_recent_touchpoint_information_final.bizible_most_recent_touchpoint_id,
        bizible_most_recent_touchpoint_information_final.bizible_most_recent_touchpoint_date,
        bizible_most_recent_touchpoint_information_final.bizible_most_recent_form_url,
        bizible_most_recent_touchpoint_information_final.bizible_most_recent_sfdc_campaign_id,
        bizible_most_recent_touchpoint_information_final.bizible_most_recent_ad_campaign_name,
        bizible_most_recent_touchpoint_information_final.bizible_most_recent_marketing_channel,
        bizible_most_recent_touchpoint_information_final.bizible_most_recent_marketing_channel_path
    FROM bizible_most_recent_touchpoint_information_final
    LEFT JOIN bizible_mql_touchpoint_information_final
        ON bizible_most_recent_touchpoint_information_final.biz_most_recent_person_id=bizible_mql_touchpoint_information_final.biz_mql_person_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2024-05-17",
    updated_date="2024-07-09",
  ) }}