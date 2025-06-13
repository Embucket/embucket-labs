{{ config(
    tags=["mnpi_exception"]
) }}

WITH account_dims_mapping AS (

  SELECT *
  FROM {{ ref('map_crm_account') }}

), crm_person AS (

    SELECT

      dim_crm_person_id,
      sfdc_record_id,
      bizible_person_id,
      bizible_touchpoint_position,
      bizible_marketing_channel_path,
      bizible_touchpoint_date,
      last_utm_content,
      last_utm_campaign,
      dim_crm_account_id,
      dim_crm_user_id,
      ga_client_id,
      person_score,
      name_of_active_sequence,
      sequence_task_due_date,
      sequence_status,
      traction_first_response_time,
      traction_first_response_time_seconds,
      traction_response_time_in_business_hours,
      last_activity_date,
      last_transfer_date_time,
      time_from_last_transfer_to_sequence,
      time_from_mql_to_last_transfer,
      zoominfo_contact_id,
      is_bdr_sdr_worked,
      is_partner_recalled,
      is_high_priority,
      high_priority_datetime,
      propensity_to_purchase_days_since_trial_start,
      propensity_to_purchase_score_date,
      last_worked_by_date,
      last_worked_by_datetime,
      
    --Groove
      groove_last_engagement_datetime,
      groove_active_flows_count,
      groove_added_to_flow_date,
      groove_flow_completed_date,
      groove_next_step_due_date,
      groove_overdue_days,
      groove_removed_from_flow_date,
      groove_engagement_score,
      groove_outbound_email_counter,

      email_hash,
      CASE
        WHEN LOWER(lead_source) LIKE '%trial - gitlab.com%' THEN TRUE
        WHEN LOWER(lead_source) LIKE '%trial - enterprise%' THEN TRUE
        ELSE FALSE
      END                                                        AS is_lead_source_trial,
      dim_account_demographics_hierarchy_sk

    FROM {{ref('prep_crm_person')}}

), account_history_final AS (
 
  SELECT
    account_id_18 AS dim_crm_account_id,
    owner_id AS dim_crm_user_id,
    ultimate_parent_id AS dim_crm_parent_account_id,
    abm_tier_1_date,
    abm_tier_2_date,
    abm_tier,
    MIN(dbt_valid_from)::DATE AS valid_from,
    MAX(dbt_valid_to)::DATE AS valid_to
  FROM {{ref('sfdc_account_snapshots_source')}}
  WHERE abm_tier_1_date >= '2022-02-01'
    OR abm_tier_2_date >= '2022-02-01'
  {{dbt_utils.group_by(n=6)}}

), industry AS (

    SELECT *
    FROM {{ ref('prep_industry') }}

), bizible_marketing_channel_path AS (

    SELECT *
    FROM {{ ref('prep_bizible_marketing_channel_path') }}

), bizible_marketing_channel_path_mapping AS (

    SELECT *
    FROM {{ ref('map_bizible_marketing_channel_path') }}

), sales_segment AS (

      SELECT *
      FROM {{ ref('dim_sales_segment') }}

), sales_territory AS (

    SELECT *
    FROM {{ ref('prep_sales_territory') }}

), sfdc_contacts AS (

    SELECT *
    FROM {{ ref('sfdc_contact_source') }}
    WHERE is_deleted = 'FALSE'

), sfdc_leads AS (

    SELECT *
    FROM {{ ref('sfdc_lead_source') }}
    WHERE is_deleted = 'FALSE'

), sfdc_lead_converted AS (

    SELECT *
    FROM sfdc_leads
    WHERE is_converted
    QUALIFY ROW_NUMBER() OVER(PARTITION BY converted_contact_id ORDER BY converted_date DESC) = 1

), prep_crm_user_hierarchy AS (

    SELECT *
    FROM {{ ref('prep_crm_user_hierarchy') }}

), marketing_qualified_leads AS(

    SELECT

      {{ dbt_utils.generate_surrogate_key(['COALESCE(converted_contact_id, lead_id)','marketo_qualified_lead_datetime::timestamp']) }} AS mql_event_id,

      marketo_qualified_lead_datetime::timestamp                                                                          AS mql_event_timestamp,
      initial_marketo_mql_date_time::timestamp                                                                            AS initial_mql_event_timestamp,
      true_mql_date::timestamp                                                                                            AS legacy_mql_event_timestamp,
      mql_datetime_inferred::timestamp                                                                                    AS inferred_mql_event_timestamp,
      lead_id                                                                                                             AS sfdc_record_id,
      'lead'                                                                                                              AS sfdc_record,
      {{ dbt_utils.generate_surrogate_key(['COALESCE(converted_contact_id, lead_id)']) }}                                          AS crm_person_id,
      converted_contact_id                                                                                                AS contact_id,
      converted_account_id                                                                                                AS account_id,
      owner_id                                                                                                            AS crm_user_id,
      person_score                                                                                                        AS person_score

    FROM sfdc_leads
    WHERE marketo_qualified_lead_datetime IS NOT NULL
      OR mql_datetime_inferred IS NOT NULL

), marketing_qualified_contacts AS(

    SELECT

      {{ dbt_utils.generate_surrogate_key(['contact_id','marketo_qualified_lead_datetime::timestamp']) }}                          AS mql_event_id,
      marketo_qualified_lead_datetime::timestamp                                                                          AS mql_event_timestamp,
      initial_marketo_mql_date_time::timestamp                                                                            AS initial_mql_event_timestamp,
      true_mql_date::timestamp                                                                                            AS legacy_mql_event_timestamp,
      mql_datetime_inferred::timestamp                                                                                    AS inferred_mql_event_timestamp,
      contact_id                                                                                                          AS sfdc_record_id,
      'contact'                                                                                                           AS sfdc_record,
      {{ dbt_utils.generate_surrogate_key(['contact_id']) }}                                                                       AS crm_person_id,
      contact_id                                                                                                          AS contact_id,
      account_id                                                                                                          AS account_id,
      owner_id                                                                                                            AS crm_user_id,
      person_score                                                                                                        AS person_score

    FROM sfdc_contacts
    WHERE marketo_qualified_lead_datetime IS NOT NULL
      OR mql_datetime_inferred IS NOT NULL
    HAVING mql_event_id NOT IN (
                         SELECT mql_event_id
                         FROM marketing_qualified_leads
                         )

), mqls_unioned AS (

    SELECT *
    FROM marketing_qualified_leads

    UNION

    SELECT *
    FROM marketing_qualified_contacts

), mqls AS (

    SELECT

      crm_person_id,
      MIN(mql_event_timestamp)          AS first_mql_date,
      MAX(mql_event_timestamp)          AS last_mql_date,
      MIN(initial_mql_event_timestamp)  AS first_initial_mql_date,
      MIN(legacy_mql_event_timestamp)   AS first_legacy_mql_date,
      MAX(legacy_mql_event_timestamp)   AS last_legacy_mql_date,
      MIN(inferred_mql_event_timestamp) AS first_inferred_mql_date,
      MAX(inferred_mql_event_timestamp) AS last_inferred_mql_date,
      COUNT(*)                          AS mql_count

    FROM mqls_unioned
    GROUP BY 1

), person_final AS (

    SELECT
    -- ids
      crm_person.dim_crm_person_id    AS dim_crm_person_id,
      crm_person.sfdc_record_id       AS sfdc_record_id,
      crm_person.bizible_person_id    AS bizible_person_id,
      crm_person.ga_client_id         AS ga_client_id,
      crm_person.zoominfo_contact_id  AS zoominfo_contact_id,

     -- common dimension keys
      crm_person.dim_crm_user_id                                                                               AS dim_crm_user_id,
      crm_person.dim_crm_account_id                                                                            AS dim_crm_account_id,
      account_dims_mapping.dim_parent_crm_account_id,                                                          -- dim_parent_crm_account_id
      COALESCE(account_dims_mapping.dim_account_sales_segment_id, sales_segment.dim_sales_segment_id)          AS dim_account_sales_segment_id,
      COALESCE(account_dims_mapping.dim_account_sales_territory_id, sales_territory.dim_sales_territory_id)    AS dim_account_sales_territory_id,
      COALESCE(account_dims_mapping.dim_account_industry_id, industry.dim_industry_id)                         AS dim_account_industry_id,
      account_dims_mapping.dim_account_location_country_id,                                                    -- dim_account_location_country_id
      account_dims_mapping.dim_account_location_region_id,                                                     -- dim_account_location_region_id
      account_dims_mapping.dim_parent_sales_segment_id,                                                        -- dim_parent_sales_segment_id
      account_dims_mapping.dim_parent_sales_territory_id,                                                      -- dim_parent_sales_territory_id
      account_dims_mapping.dim_parent_industry_id,                                                             -- dim_parent_industry_id
      {{ get_keyed_nulls('bizible_marketing_channel_path.dim_bizible_marketing_channel_path_id') }}            AS dim_bizible_marketing_channel_path_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_hierarchy_id') }}                               AS dim_account_demographics_hierarchy_id,
      crm_person.dim_account_demographics_hierarchy_sk,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_sales_segment_id') }}                           AS dim_account_demographics_sales_segment_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_geo_id') }}                                     AS dim_account_demographics_geo_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_region_id') }}                                  AS dim_account_demographics_region_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_area_id') }}                                    AS dim_account_demographics_area_id,

     -- important person dates
      COALESCE(sfdc_leads.created_date, sfdc_lead_converted.created_date, sfdc_contacts.created_date)::DATE
                                                                                                                AS created_date,
      {{ get_date_id('COALESCE(sfdc_leads.created_date, sfdc_lead_converted.created_date, sfdc_contacts.created_date)') }}
                                                                                                                AS created_date_id,
      {{ get_date_pt_id('COALESCE(sfdc_leads.created_date, sfdc_lead_converted.created_date, sfdc_contacts.created_date)') }}
                                                                                                                AS created_date_pt_id,
      COALESCE(sfdc_leads.created_date, sfdc_lead_converted.created_date)::DATE                                 AS lead_created_date,
      {{ get_date_id('COALESCE(sfdc_leads.created_date, sfdc_lead_converted.created_date)::DATE') }}            AS lead_created_date_id,
      {{ get_date_pt_id('COALESCE(sfdc_leads.created_date, sfdc_lead_converted.created_date)::DATE') }}         AS lead_created_date_pt_id,
      sfdc_contacts.created_date::DATE                                                                          AS contact_created_date,
      {{ get_date_id('sfdc_contacts.created_date::DATE') }}                                                     AS contact_created_date_id,
      {{ get_date_pt_id('sfdc_contacts.created_date::DATE') }}                                                  AS contact_created_date_pt_id,
      COALESCE(sfdc_contacts.inquiry_datetime, sfdc_leads.inquiry_datetime)::DATE                               AS inquiry_date,
      {{ get_date_id('inquiry_date') }}                                                                         AS inquiry_date_id,
      {{ get_date_pt_id('inquiry_date') }}                                                                      AS inquiry_date_pt_id,
      COALESCE(sfdc_contacts.inquiry_datetime_inferred, sfdc_leads.inquiry_datetime_inferred)::DATE             AS inquiry_inferred_datetime,
      {{ get_date_id('inquiry_inferred_datetime') }}                                                            AS inquiry_inferred_datetime_id,
      {{ get_date_pt_id('inquiry_inferred_datetime') }}                                                         AS inquiry_inferred_datetime_pt_id,
      LEAST(COALESCE(inquiry_date,'9999-01-01'),COALESCE(inquiry_inferred_datetime,'9999-01-01'))               AS prep_true_inquiry_date,
      CASE 
        WHEN prep_true_inquiry_date != '9999-01-01'
        THEN prep_true_inquiry_date
      END                                                                                                       AS true_inquiry_date,
      {{ get_date_id('true_inquiry_date') }}                                                                    AS true_inquiry_date_id,
      {{ get_date_pt_id('true_inquiry_date') }}                                                                 AS true_inquiry_date_pt_id,
      mqls.first_mql_date::DATE                                                                                 AS mql_date_first,
      mqls.first_mql_date                                                                                       AS mql_datetime_first,
      CONVERT_TIMEZONE('America/Los_Angeles', mqls.first_mql_date)                                              AS mql_datetime_first_pt,
      {{ get_date_id('first_mql_date') }}                                                                       AS mql_date_first_id,
      {{ get_date_pt_id('first_mql_date') }}                                                                    AS mql_date_first_pt_id,
      mqls.last_mql_date::DATE                                                                                  AS mql_date_latest,
      mqls.last_mql_date                                                                                        AS mql_datetime_latest,
      CONVERT_TIMEZONE('America/Los_Angeles', mqls.last_mql_date)                                               AS mql_datetime_latest_pt,
      {{ get_date_id('last_mql_date') }}                                                                        AS mql_date_latest_id,
      {{ get_date_pt_id('last_mql_date') }}                                                                     AS mql_date_latest_pt_id,
      mqls.first_initial_mql_date::DATE                                                                         AS initial_mql_date_first,
      mqls.first_initial_mql_date                                                                               AS initial_mql_datetime_first,
      CONVERT_TIMEZONE('America/Los_Angeles', mqls.first_initial_mql_date)                                      AS initial_mql_datetime_first_pt,
      {{ get_date_id('first_initial_mql_date') }}                                                               AS initial_mql_date_first_id,
      {{ get_date_pt_id('first_initial_mql_date') }}                                                            AS initial_mql_date_first_pt_id,

      mqls.first_legacy_mql_date::DATE                                                                          AS legacy_mql_date_first,
      mqls.first_legacy_mql_date                                                                                AS legacy_mql_datetime_first,
      CONVERT_TIMEZONE('America/Los_Angeles', mqls.first_legacy_mql_date)                                       AS legacy_mql_datetime_first_pt,
      {{ get_date_id('legacy_mql_date_first') }}                                                                AS legacy_mql_date_first_id,
      {{ get_date_pt_id('legacy_mql_date_first') }}                                                             AS legacy_mql_date_first_pt_id,
      mqls.last_legacy_mql_date::DATE                                                                           AS legacy_mql_date_latest,
      mqls.last_legacy_mql_date                                                                                 AS legacy_mql_datetime_latest,
      CONVERT_TIMEZONE('America/Los_Angeles', mqls.last_legacy_mql_date)                                        AS legacy_mql_datetime_latest_pt,
      {{ get_date_id('last_legacy_mql_date') }}                                                                 AS legacy_mql_date_latest_id,
      {{ get_date_pt_id('last_legacy_mql_date') }}                                                              AS legacy_mql_date_latest_pt_id,

      mqls.first_inferred_mql_date::DATE                                                                        AS inferred_mql_date_first,
      mqls.first_inferred_mql_date                                                                              AS inferred_mql_datetime_first,
      CONVERT_TIMEZONE('America/Los_Angeles', mqls.first_inferred_mql_date)                                     AS inferred_mql_datetime_first_pt,
      {{ get_date_id('first_inferred_mql_date') }}                                                              AS inferred_mql_date_first_id,
      {{ get_date_pt_id('first_inferred_mql_date') }}                                                           AS inferred_mql_date_first_pt_id,
      mqls.last_inferred_mql_date::DATE                                                                         AS inferred_mql_date_latest,
      mqls.last_inferred_mql_date                                                                               AS inferred_mql_datetime_latest,
      CONVERT_TIMEZONE('America/Los_Angeles', mqls.last_inferred_mql_date)                                      AS inferred_mql_datetime_latest_pt,
      {{ get_date_id('last_inferred_mql_date') }}                                                               AS inferred_mql_date_latest_id,
      {{ get_date_pt_id('last_inferred_mql_date') }}                                                            AS inferred_mql_date_latest_pt_id,

      COALESCE(sfdc_contacts.marketo_qualified_lead_datetime, sfdc_leads.marketo_qualified_lead_datetime)::DATE 
                                                                                                                AS mql_sfdc_date,
      COALESCE(sfdc_contacts.marketo_qualified_lead_datetime, sfdc_leads.marketo_qualified_lead_datetime)       AS mql_sfdc_datetime,
      {{ get_date_id('mql_sfdc_date') }}                                                                        AS mql_sfdc_date_id,
      {{ get_date_pt_id('mql_sfdc_date') }}                                                                     AS mql_sfdc_date_pt_id,
      COALESCE(sfdc_contacts.mql_datetime_inferred, sfdc_leads.mql_datetime_inferred)::DATE                     AS mql_inferred_date,
      COALESCE(sfdc_contacts.mql_datetime_inferred, sfdc_leads.mql_datetime_inferred)                           AS mql_inferred_datetime,
      {{ get_date_id('mql_inferred_date') }}                                                                    AS mql_inferred_date_id,
      {{ get_date_pt_id('mql_inferred_date') }}                                                                 AS mql_inferred_date_pt_id,
      COALESCE(sfdc_contacts.accepted_datetime, sfdc_leads.accepted_datetime)::DATE                             AS accepted_date,
      COALESCE(sfdc_contacts.accepted_datetime, sfdc_leads.accepted_datetime)                                   AS accepted_datetime,
      CONVERT_TIMEZONE('America/Los_Angeles', COALESCE(sfdc_contacts.accepted_datetime, sfdc_leads.accepted_datetime))
                                                                                                                AS accepted_datetime_pt,
      {{ get_date_id('accepted_date') }}                                                                        AS accepted_date_id,
      {{ get_date_pt_id('accepted_date') }}                                                                     AS accepted_date_pt_id,
      COALESCE(sfdc_contacts.qualifying_datetime, sfdc_leads.qualifying_datetime)::DATE                         AS qualifying_date,
      {{ get_date_id('qualifying_date') }}                                                                      AS qualifying_date_id,
      {{ get_date_pt_id('qualifying_date') }}                                                                   AS qualifying_date_pt_id,
      COALESCE(sfdc_contacts.qualified_datetime, sfdc_leads.qualified_datetime)::DATE                           AS qualified_date,
      {{ get_date_id('qualified_date') }}                                                                       AS qualified_date_id,
      {{ get_date_pt_id('qualified_date') }}                                                                    AS qualified_date_pt_id,
      COALESCE(sfdc_contacts.initial_recycle_datetime, sfdc_leads.initial_recycle_datetime)::DATE               AS initial_recycle_date,
      COALESCE(sfdc_contacts.initial_recycle_datetime, sfdc_leads.initial_recycle_datetime)                     AS initial_recycle_datetime,
      {{ get_date_id('initial_recycle_date') }}                                                                 AS initial_recycle_date_id,
      {{ get_date_pt_id('initial_recycle_date') }}                                                              AS initial_recycle_date_pt_id,
      COALESCE(sfdc_contacts.most_recent_recycle_datetime, sfdc_leads.most_recent_recycle_datetime)::DATE       AS most_recent_recycle_date,
      {{ get_date_id('most_recent_recycle_date') }}                                                             AS most_recent_recycle_date_id,
      {{ get_date_pt_id('most_recent_recycle_date') }}                                                          AS most_recent_recycle_date_pt_id,
      sfdc_lead_converted.converted_date::DATE                                                                  AS converted_date,
      {{ get_date_id('sfdc_lead_converted.converted_date') }}                                                   AS converted_date_id,
      {{ get_date_pt_id('sfdc_lead_converted.converted_date') }}                                                AS converted_date_pt_id,
      COALESCE(sfdc_contacts.worked_datetime, sfdc_leads.worked_datetime)::DATE                                 AS worked_date,
      {{ get_date_id('worked_date') }}                                                                          AS worked_date_id,
      {{ get_date_pt_id('worked_date') }}                                                                       AS worked_date_pt_id,
      crm_person.high_priority_datetime,
     -- flags
      CASE
          WHEN mqls.last_mql_date IS NOT NULL THEN 1
          ELSE 0
        END                                                                                                               AS is_mql,
      CASE
        WHEN true_inquiry_date IS NOT NULL THEN 1
        ELSE 0
      END                                                                                                                 AS is_inquiry,
      crm_person.is_bdr_sdr_worked,
      crm_person.is_partner_recalled,
      crm_person.is_lead_source_trial,
      crm_person.is_high_priority,
      crm_person.last_worked_by_date,
      crm_person.last_worked_by_datetime,

    --Groove
      crm_person.groove_last_engagement_datetime,
      crm_person.groove_active_flows_count,
      crm_person.groove_added_to_flow_date,
      crm_person.groove_flow_completed_date,
      crm_person.groove_next_step_due_date,
      crm_person.groove_overdue_days,
      crm_person.groove_removed_from_flow_date,
      crm_person.groove_engagement_score,
      crm_person.groove_outbound_email_counter,

     -- information fields
      crm_person.name_of_active_sequence,
      crm_person.sequence_task_due_date,
      crm_person.sequence_status,
      crm_person.last_activity_date,
      crm_person.last_utm_content,
      crm_person.last_utm_campaign,
      crm_person.last_transfer_date_time,
      crm_person.time_from_last_transfer_to_sequence,
      crm_person.time_from_mql_to_last_transfer,
      crm_person.traction_first_response_time,
      crm_person.traction_first_response_time_seconds,
      crm_person.traction_response_time_in_business_hours,
      crm_person.propensity_to_purchase_score_date,
      crm_person.propensity_to_purchase_days_since_trial_start,
      crm_person.email_hash,

     -- additive fields

      crm_person.person_score                                                                                             AS person_score,
      mqls.mql_count                                                                                                      AS mql_count
      

    FROM crm_person
    LEFT JOIN sfdc_leads
      ON crm_person.sfdc_record_id = sfdc_leads.lead_id
    LEFT JOIN sfdc_contacts
      ON crm_person.sfdc_record_id = sfdc_contacts.contact_id
    LEFT JOIN sfdc_lead_converted
      ON crm_person.sfdc_record_id = sfdc_lead_converted.converted_contact_id
    LEFT JOIN mqls
      ON crm_person.dim_crm_person_id = mqls.crm_person_id
    LEFT JOIN account_dims_mapping
      ON crm_person.dim_crm_account_id = account_dims_mapping.dim_crm_account_id
    LEFT JOIN sales_segment
      ON sfdc_leads.sales_segmentation = sales_segment.sales_segment_name
    LEFT JOIN sales_territory
      ON sfdc_leads.tsp_territory = sales_territory.sales_territory_name
    LEFT JOIN industry
      ON COALESCE(sfdc_contacts.industry, sfdc_leads.industry) = industry.industry_name
    LEFT JOIN bizible_marketing_channel_path_mapping
      ON crm_person.bizible_marketing_channel_path = bizible_marketing_channel_path_mapping.bizible_marketing_channel_path
    LEFT JOIN bizible_marketing_channel_path
      ON bizible_marketing_channel_path_mapping.bizible_marketing_channel_path_name_grouped = bizible_marketing_channel_path.bizible_marketing_channel_path_name
    LEFT JOIN prep_crm_user_hierarchy
      ON prep_crm_user_hierarchy.dim_crm_user_hierarchy_sk = crm_person.dim_account_demographics_hierarchy_sk

), inquiry_base AS (
  
  SELECT
  --IDs
    person_final.dim_crm_person_id,

  --Person Data
    person_final.true_inquiry_date,
    -- account_history_final.abm_tier_1_date,
    -- account_history_final.abm_tier_2_date,
    -- account_history_final.abm_tier,
    CASE 
      WHEN true_inquiry_date IS NOT NULL
        AND true_inquiry_date BETWEEN valid_from AND valid_to
        THEN TRUE
      ELSE FALSE
    END AS is_abm_tier_inquiry
  FROM person_final
  LEFT JOIN account_history_final
    ON person_final.dim_crm_account_id=account_history_final.dim_crm_account_id
  WHERE abm_tier IS NOT NULL
  AND true_inquiry_date IS NOT NULL
  AND true_inquiry_date >= '2022-02-01'
  AND (abm_tier_1_date IS NOT NULL
    OR abm_tier_2_date IS NOT NULL)
  AND is_abm_tier_inquiry = TRUE
  
), mql_base AS (
  
  SELECT
  --IDs
    person_final.dim_crm_person_id,
  
  --Person Data
    person_final.mql_datetime_latest_pt,
    -- account_history_final.abm_tier_1_date,
    -- account_history_final.abm_tier_2_date,
    -- account_history_final.abm_tier,
    CASE 
      WHEN mql_datetime_latest_pt IS NOT NULL
        AND mql_datetime_latest_pt BETWEEN valid_from AND valid_to
        THEN TRUE
      ELSE FALSE
    END AS is_abm_tier_mql  
  FROM person_final
  LEFT JOIN account_history_final
    ON person_final.dim_crm_account_id=account_history_final.dim_crm_account_id
  WHERE abm_tier IS NOT NULL
  AND mql_datetime_latest_pt IS NOT NULL
  AND mql_datetime_latest_pt >= '2022-02-01'
  AND (abm_tier_1_date IS NOT NULL
    OR abm_tier_2_date IS NOT NULL)
  AND is_abm_tier_mql = TRUE

), abm_tier_id AS (

    SELECT
        dim_crm_person_id
    FROM inquiry_base
    UNION ALL
    SELECT
        dim_crm_person_id
    FROM mql_base

), abm_tier_id_final AS (

    SELECT DISTINCT
        dim_crm_person_id
    FROM abm_tier_id

), abm_tier_final AS (
  
  SELECT DISTINCT
    abm_tier_id_final.dim_crm_person_id,
    inquiry_base.is_abm_tier_inquiry,
    mql_base.is_abm_tier_mql
  FROM abm_tier_id_final
  LEFT JOIN inquiry_base
      ON abm_tier_id_final.dim_crm_person_id=inquiry_base.dim_crm_person_id
  LEFT JOIN mql_base
    ON abm_tier_id_final.dim_crm_person_id=mql_base.dim_crm_person_id

), final AS (

  SELECT
    person_final.*,
    abm_tier_final.is_abm_tier_inquiry,
    abm_tier_final.is_abm_tier_mql
  FROM person_final
  LEFT JOIN abm_tier_final
    ON person_final.dim_crm_person_id=abm_tier_final.dim_crm_person_id
  

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@rkohnke",
    created_date="2020-12-01",
    updated_date="2024-10-07"
) }}
