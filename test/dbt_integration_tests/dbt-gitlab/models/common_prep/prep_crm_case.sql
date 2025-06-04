WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_case_source') }}

), renamed AS(

    SELECT
      case_id,

      --keys
      {{ dbt_utils.generate_surrogate_key(['source.case_id']) }} AS dim_crm_case_sk,
      --source.case_id AS dim_crm_case_pk,
      source.account_id                                          AS dim_crm_account_id,
      source.business_hours_id,
      source.case_number,
      source.contact_id,
      source.from_chatter_feed_id,
      source.owner_id AS dim_crm_user_id,
      source.record_type_id,
      source.source_id,
      source.opportunity_id AS dim_crm_opportunity_id,
      source.created_by_id,
      source.dr_deal_id,

      --Case Information
      source.closed_date,
      source.description,
      source.is_closed,
      source.is_closed_on_create,
      source.is_escalated,
      source.origin,
      source.priority,
      source.reason,
      source.resolution_action,
      source.status,
      source.subject,
      source.case_type,
      source.created_date,
      source.spam_checkbox,
      source.context,
      source.feedback,
      source.feedback_type,
      source.first_responded_date,
      source.time_to_first_response,
      source.next_steps,
      source.next_steps_date,
      source.is_deleted,
      source.primary_deal_registration,
      source.case_close_reason,
      source.first_case_owner_name,

      --Contact Information
      source.contact_email,
      source.contact_fax,
      source.contact_mobile,
      source.contact_phone,
      source.supplied_company,
      source.supplied_email,
      source.supplied_name,
      source.supplied_phone,

      --Case age information
      DATEDIFF('DAY',source.created_date,source.closed_date) AS days_create_to_close,
      DATEDIFF('DAY',source.created_date,CURRENT_DATE)       AS days_create_to_today,
      source.days_since_last_activity,


      --metadata
      source.last_modified_by_id,
      source.last_modified_date,
      source.systemmodstamp

    FROM source
)

   {{ dbt_audit(
    cte_ref="renamed",
    created_by="@mfleisher",
    updated_by="@snalamaru",
    created_date="2024-06-10",
    updated_date="2024-06-12"
) }}