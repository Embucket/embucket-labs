{{ config(
    tags=["mnpi_exception"]
) }}

WITH prep_crm_event AS (

  SELECT *
  FROM {{ ref('prep_crm_event') }} 
  WHERE is_deleted = FALSE

 ), converted_leads AS (
-- Original CRM Event table would show null keyed id for dim_crm_person_id for leads that have been converted to contacts
-- This CTE is pulling the most recent sfdc_record_id and dim_crm_person_id of a record so that they correspond to the values in the mart_crm_person model.
  SELECT
    sfdc_lead_source.converted_contact_id AS sfdc_record_id,
    sfdc_lead_source.lead_id,
    prep_crm_person.dim_crm_person_id
  FROM {{ ref('sfdc_lead_source') }} 
  LEFT JOIN {{ ref('prep_crm_person') }} 
    ON sfdc_lead_source.converted_contact_id=prep_crm_person.sfdc_record_id
  WHERE sfdc_lead_source.is_converted = TRUE

), final AS (

  SELECT
    -- Primary key
    prep_crm_event.dim_crm_event_pk,

    -- Foreign keys
    {{ get_keyed_nulls('prep_crm_event.dim_crm_event_sk') }}       AS dim_crm_event_sk,
    {{ get_keyed_nulls('prep_crm_event.dim_crm_account_id') }}     AS dim_crm_account_id,
    {{ get_keyed_nulls('prep_crm_event.dim_crm_user_id') }}        AS dim_crm_user_id,
    {{ get_keyed_nulls('prep_crm_person.dim_crm_person_id') }}     AS dim_crm_person_id,
    {{ get_keyed_nulls('prep_crm_event.dim_crm_opportunity_id') }} AS dim_crm_opportunity_id,
    COALESCE(converted_leads.sfdc_record_id,prep_crm_event.sfdc_record_id) 
                                                                   AS sfdc_record_id,

    -- Dates
    {{ get_date_id('prep_crm_event.event_date') }}                 AS event_date_id,
    prep_crm_event.event_date,
    {{ get_date_id('prep_crm_event.event_end_date') }}             AS event_end_date_id,
    prep_crm_event.event_end_date,
    {{ get_date_id('prep_crm_event.reminder_date_time') }}         AS reminder_date_id,
    prep_crm_event.reminder_date_time,
    {{ get_date_id('prep_crm_event.event_recurrence_end_date') }}  AS event_recurrence_end_date_id,
    prep_crm_event.event_recurrence_end_date,
    {{ get_date_id('prep_crm_event.event_recurrence_start_date_time') }} AS event_recurrence_start_date_id,
    prep_crm_event.event_recurrence_start_date_time,


    -- Metadata
    prep_crm_event.created_by_id                                   AS event_created_by_id,
    {{ get_date_id('prep_crm_event.created_at') }}                 AS event_created_date_id,
    prep_crm_event.created_at                                      AS event_created_date,
    prep_crm_event.last_modified_id,
    {{ get_date_id('prep_crm_event.last_modified_date') }}         AS last_modified_date_id,
    prep_crm_event.last_modified_date
  FROM prep_crm_event
  LEFT JOIN converted_leads
    ON prep_crm_event.sfdc_record_id=converted_leads.lead_id
  LEFT JOIN {{ ref('prep_crm_person') }}
    ON COALESCE(converted_leads.sfdc_record_id,prep_crm_event.sfdc_record_id) = prep_crm_person.sfdc_record_id


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-08-22",
    updated_date="2024-07-24"
) }}