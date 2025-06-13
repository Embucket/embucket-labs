{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('prep_crm_person', 'prep_crm_person'),
    ('sfdc_lead_source','sfdc_lead_source')
]) }}
    
, prep_crm_task AS (

  SELECT *
  FROM {{ ref('prep_crm_task') }} 
  WHERE is_deleted = FALSE

), prep_crm_opportunity AS (

  SELECT *
  FROM {{ ref('prep_crm_opportunity') }}
  WHERE is_deleted = FALSE

), account_opp_mapping AS (
-- Maps opportunities to tasks based on the account of task (account_opp_mapping) where prep_crm_task.dim_crm_opportunity_id = prep_crm_opportunity.dim_crm_opportunity_id fails
-- It uses the opp chronologically closest to the task date (rank_closest_opp) between three quarters prior to the (opp) close date and the (opp) close date.
  SELECT 
    prep_crm_task.dim_crm_task_pk AS dim_crm_task_pk,

     COALESCE(prep_crm_task.dim_crm_opportunity_id, prep_crm_opportunity.dim_crm_opportunity_id) AS dim_mapped_opportunity_id,

    CASE 
        WHEN prep_crm_task.dim_crm_opportunity_id IS NOT NULL 
          THEN 'Opportunity'
        WHEN prep_crm_task.dim_crm_opportunity_id IS NULL 
          THEN 'Account'
        ELSE 'Not Mappable'
        END AS task_mapped_to,

    ROW_NUMBER() OVER(PARTITION BY prep_crm_task.dim_crm_task_pk 
      ORDER BY DATEDIFF('day', prep_crm_task.task_date, prep_crm_opportunity.close_date) ASC
      , prep_crm_opportunity.net_arr DESC) 
    AS rank_closest_opp
  
  FROM prep_crm_opportunity
  JOIN prep_crm_task ON 
      prep_crm_task.dim_crm_account_id = prep_crm_opportunity.dim_crm_account_id
WHERE prep_crm_task.sa_activity_type IS NOT NULL
  AND DATE_TRUNC('day',prep_crm_task.task_date) <= prep_crm_opportunity.close_date

  ), opps_ranked AS (

    SELECT DISTINCT
      dim_crm_task_pk,
      dim_mapped_opportunity_id,
      task_mapped_to

    FROM account_opp_mapping
    WHERE rank_closest_opp = 1

  ), converted_leads AS (
-- Original CRM Task table would show null keyed id for dim_crm_person_id for leads that have been converted to contacts
-- This CTE is pulling the most recent sfdc_record_id and dim_crm_person_id of a record so that they correspond to the values in the mart_crm_person model.
  SELECT
    sfdc_lead_source.converted_contact_id AS sfdc_record_id,
    sfdc_lead_source.lead_id,
    prep_crm_person.dim_crm_person_id
  FROM sfdc_lead_source
  LEFT JOIN prep_crm_person
    ON sfdc_lead_source.converted_contact_id=prep_crm_person.sfdc_record_id
  WHERE sfdc_lead_source.is_converted = TRUE

), final AS (

  SELECT
    -- Primary key
    prep_crm_task.dim_crm_task_pk,

    -- Foreign keys
    {{ get_keyed_nulls('prep_crm_task.dim_crm_task_sk') }}        AS dim_crm_task_sk,
    {{ get_keyed_nulls('prep_crm_task.dim_crm_account_id') }}     AS dim_crm_account_id,
    {{ get_keyed_nulls('prep_crm_task.dim_crm_user_id') }}        AS dim_crm_user_id,
    {{ get_keyed_nulls('prep_crm_task.sfdc_record_type_id') }}    AS sfdc_record_type_id,
    {{ get_keyed_nulls('prep_crm_person.dim_crm_person_id') }}    AS dim_crm_person_id,
    {{ get_keyed_nulls('prep_crm_task.dim_crm_opportunity_id') }} AS dim_crm_opportunity_id,
    opps_ranked.dim_mapped_opportunity_id,

    COALESCE(converted_leads.sfdc_record_id,prep_crm_task.sfdc_record_id) 
                                                                  AS sfdc_record_id,

    -- Dates
    {{ get_date_id('prep_crm_task.task_date') }}                  AS task_date_id,
    prep_crm_task.task_date,
    {{ get_date_id('prep_crm_task.task_completed_date') }}        AS task_completed_date_id,
    prep_crm_task.task_completed_date,
    {{ get_date_id('prep_crm_task.reminder_date') }}              AS reminder_date_id,
    prep_crm_task.reminder_date,
    {{ get_date_id('prep_crm_task.task_recurrence_date') }}       AS task_recurrence_date_id,
    prep_crm_task.task_recurrence_date,
    {{ get_date_id('prep_crm_task.task_recurrence_start_date') }} AS task_recurrence_start_date_id,
    prep_crm_task.task_recurrence_start_date,

    -- Logic
    opps_ranked.task_mapped_to,

    -- Counts
    prep_crm_task.account_or_opportunity_count,
    prep_crm_task.lead_or_contact_count,

    -- Task durations
    prep_crm_task.hours_waiting_before_task,
    prep_crm_task.hours_waiting_before_email_task,
    prep_crm_task.call_task_duration_in_seconds,
    prep_crm_task.hours_waiting_before_call_task,

    -- Metadata
    prep_crm_task.task_created_by_id,
    {{ get_date_id('prep_crm_task.task_created_date') }}          AS task_created_date_id,
    prep_crm_task.task_created_date,
    prep_crm_task.last_modified_id,
    {{ get_date_id('prep_crm_task.last_modified_date') }}         AS last_modified_date_id,
    prep_crm_task.last_modified_date
  FROM prep_crm_task
  LEFT JOIN converted_leads
    ON prep_crm_task.sfdc_record_id=converted_leads.lead_id
  LEFT JOIN prep_crm_person
    ON COALESCE(converted_leads.sfdc_record_id,prep_crm_task.sfdc_record_id) = prep_crm_person.sfdc_record_id
  LEFT JOIN opps_ranked
    ON prep_crm_task.dim_crm_task_pk = opps_ranked.dim_crm_task_pk

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@jngCES",
    created_date="2022-12-05",
    updated_date="2023-11-20"
) }}
