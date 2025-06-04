{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_crm_event', 'dim_crm_event'),
    ('fct_crm_event', 'fct_crm_event')
]) }}

, final AS (


  SELECT
    -- Primary Key
    fct_crm_event.dim_crm_event_pk,

    -- Surrogate key
    dim_crm_event.dim_crm_event_sk,

    -- Natural key
    dim_crm_event.event_id,

    -- Foreign keys
    fct_crm_event.dim_crm_account_id,
    fct_crm_event.dim_crm_user_id,
    fct_crm_event.dim_crm_person_id,
    fct_crm_event.sfdc_record_id,
    fct_crm_event.dim_crm_opportunity_id,

    --Event Information
    dim_crm_event.event_subject,
    dim_crm_event.event_source,
    dim_crm_event.outreach_meeting_type,
    dim_crm_event.event_type,
    dim_crm_event.event_disposition,
    dim_crm_event.event_description, 
    dim_crm_event.event_subtype,
    dim_crm_event.booked_by_employee_number,
    dim_crm_event.sa_activity_type,
    dim_crm_event.event_show_as,
    dim_crm_event.assigned_to_role,
    dim_crm_event.csm_activity_type,
    dim_crm_event.customer_interaction_sentiment,
    dim_crm_event.google_doc_link,
    dim_crm_event.comments,
    dim_crm_event.qualified_convo_or_meeting,
    dim_crm_event.first_opportunity_event_created_date,
    dim_crm_event.partner_marketing_task_subject,

    --sales dev hierarchy fields
    dim_sales_dev_user_hierarchy.sales_dev_rep_user_full_name,
    dim_sales_dev_user_hierarchy.sales_dev_rep_manager_full_name,
    dim_sales_dev_user_hierarchy.sales_dev_rep_leader_full_name,
    dim_sales_dev_user_hierarchy.sales_dev_rep_user_role_level_1,
    dim_sales_dev_user_hierarchy.sales_dev_rep_user_role_level_2,
    dim_sales_dev_user_hierarchy.sales_dev_rep_user_role_level_3,

    --Dates and Datetimes
    dim_crm_event.event_start_date_time,
    dim_crm_event.event_end_date_time,
    dim_crm_event.event_date_time,
    dim_crm_event.created_at,
    fct_crm_event.event_date_id,
    fct_crm_event.event_date,
    fct_crm_event.event_end_date_id,
    fct_crm_event.event_end_date,
    fct_crm_event.reminder_date_id,
    fct_crm_event.reminder_date_time,
    fct_crm_event.event_recurrence_end_date_id,
    fct_crm_event.event_recurrence_end_date,
    fct_crm_event.event_recurrence_start_date_id,
    fct_crm_event.event_recurrence_start_date_time,   

    -- Metadata
    fct_crm_event.event_created_by_id,
    fct_crm_event.event_created_date_id,
    fct_crm_event.event_created_date,
    fct_crm_event.last_modified_id,
    fct_crm_event.last_modified_date_id,
    fct_crm_event.last_modified_date

    FROM fct_crm_event 
    LEFT JOIN dim_crm_event 
      ON fct_crm_event.dim_crm_event_sk = dim_crm_event.dim_crm_event_sk
    LEFT JOIN {{ref('dim_sales_dev_user_hierarchy')}}
      ON fct_crm_event.dim_crm_user_id=dim_sales_dev_user_hierarchy.dim_crm_user_id
        AND fct_crm_event.event_date=dim_sales_dev_user_hierarchy.snapshot_date


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-08-22",
    updated_date="2024-07-31"
) }}