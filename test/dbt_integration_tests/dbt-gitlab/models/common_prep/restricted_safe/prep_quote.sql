WITH sfdc_zqu_quote_source AS (

    SELECT *
    FROM {{ ref('sfdc_zqu_quote_source') }}
    WHERE is_deleted = FALSE

), final AS (

    SELECT
      zqu_quote_id                                  AS dim_quote_id,
      zqu__number                                   AS quote_number,
      zqu_quote_name                                AS quote_name,
      zqu__status                                   AS quote_status,
      zqu__primary                                  AS is_primary_quote,
      quote_entity,
      zqu__start_date                               AS quote_start_date,
      created_date,
      zqu__subscriptiontype                         AS subscription_action_type,
      CASE
        WHEN LOWER(submitter_comments) LIKE '%training%'
            OR LOWER(submitter_comments) LIKE '%certification%'
            OR LOWER(submitter_comments) LIKE '%education%'
        THEN TRUE
        ELSE FALSE
      END AS is_education_exemption_comment,
      CASE
        WHEN LOWER(required_approvals_from_asm) LIKE '%training%'
            OR LOWER(required_approvals_from_asm) LIKE '%certification%'
            OR LOWER(required_approvals_from_asm) LIKE '%education%'
            OR LOWER(required_approvals_from_vp_of_sales_rd) LIKE '%training%'
            OR LOWER(required_approvals_from_vp_of_sales_rd) LIKE '%certification%'
            OR LOWER(required_approvals_from_vp_of_sales_rd) LIKE '%education%'
            OR LOWER(required_approvals_from_cro) LIKE '%training%'
            OR LOWER(required_approvals_from_cro) LIKE '%certification%'
            OR LOWER(required_approvals_from_cro) LIKE '%education%'
            OR LOWER(required_approvals_from_cs) LIKE '%training%'
            OR LOWER(required_approvals_from_cs) LIKE '%certification%'
            OR LOWER(required_approvals_from_cs) LIKE '%education%'
            OR LOWER(required_approvals_from_rd) LIKE '%training%'
            OR LOWER(required_approvals_from_rd) LIKE '%certification%'
            OR LOWER(required_approvals_from_rd) LIKE '%education%'
            OR LOWER(required_approvals_from_vp_of_channel) LIKE '%training%'
            OR LOWER(required_approvals_from_vp_of_channel) LIKE '%certification%'
            OR LOWER(required_approvals_from_vp_of_channel) LIKE '%education%'
            OR LOWER(required_approvals_from_ceo) LIKE '%training%'
            OR LOWER(required_approvals_from_ceo) LIKE '%certification%'
            OR LOWER(required_approvals_from_ceo) LIKE '%education%'
            OR LOWER(required_approvals_from_cfo) LIKE '%training%'
            OR LOWER(required_approvals_from_cfo) LIKE '%certification%'
            OR LOWER(required_approvals_from_cfo) LIKE '%education%'
            OR LOWER(required_approvals_from_legal) LIKE '%training%'
            OR LOWER(required_approvals_from_legal) LIKE '%certification%'
            OR LOWER(required_approvals_from_legal) LIKE '%education%'
            OR LOWER(required_approvals_from_enterprise_vp) LIKE '%training%'
            OR LOWER(required_approvals_from_enterprise_vp) LIKE '%certification%'
            OR LOWER(required_approvals_from_enterprise_vp) LIKE '%education%'
            THEN TRUE
          ELSE FALSE
      END AS is_education_approval_required
    FROM sfdc_zqu_quote_source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@rkohnke",
    created_date="2021-01-07",
    updated_date="2024-10-28"
) }}