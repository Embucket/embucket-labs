{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
      ('map_merged_crm_account', 'map_merged_crm_account'),
      ('date_details', 'date_details'),
      ('zuora_account_source', 'zuora_account_source'),
      ('prep_billing_account_user', 'prep_billing_account_user'),
      ('sfdc_customer_subscription_source', 'sfdc_customer_subscription_source')

]) }},

zuora_subscription AS (

  SELECT *
  FROM {{ ref('zuora_subscription_source') }}
  WHERE is_deleted = FALSE
    AND exclude_from_analysis IN ('False', '')

),

joined AS (

  SELECT
    zuora_subscription.subscription_id                                                                                                                                                            AS dim_subscription_id,
    map_merged_crm_account.dim_crm_account_id,
    zuora_account_source.account_id                                                                                                                                                               AS dim_billing_account_id,
    zuora_subscription.invoice_owner_id                                                                                                                                                           AS dim_billing_account_id_invoice_owner_account,
    zuora_subscription.creator_account_id                                                                                                                                                         AS dim_billing_account_id_creator_account,
    zuora_subscription.sfdc_opportunity_id                                                                                                                                                        AS dim_crm_opportunity_id,
    sfdc_customer_subscription_source.current_open_renewal_id                                                                                                                                     AS dim_crm_opportunity_id_current_open_renewal,
    sfdc_customer_subscription_source.closed_lost_renewal_id                                                                                                                                      AS dim_crm_opportunity_id_closed_lost_renewal,
    zuora_subscription.original_id                                                                                                                                                                AS dim_subscription_id_original,
    zuora_subscription.previous_subscription_id                                                                                                                                                   AS dim_subscription_id_previous,
    zuora_subscription.amendment_id                                                                                                                                                               AS dim_amendment_id_subscription,
    zuora_subscription.created_by_id,
    zuora_subscription.updated_by_id,
    zuora_subscription.subscription_name,
    zuora_subscription.subscription_name_slugify,
    zuora_subscription.subscription_status,
    zuora_subscription.version                                                                                                                                                                    AS subscription_version,
    zuora_subscription.zuora_renewal_subscription_name,
    zuora_subscription.zuora_renewal_subscription_name_slugify,
    zuora_subscription.current_term,
    zuora_subscription.renewal_term,
    zuora_subscription.renewal_term_period_type,
    zuora_subscription.eoa_starter_bronze_offer_accepted,
    CASE
      WHEN prep_billing_account_user.is_integration_user = 1
        THEN 'Self-Service'
      ELSE 'Sales-Assisted'
    END                                                                                                                                                                                           AS subscription_sales_type,
    zuora_subscription.namespace_name,
    zuora_subscription.namespace_id,
    invoice_owner.account_name                                                                                                                                                                    AS invoice_owner_account,
    creator_account.account_name                                                                                                                                                                  AS creator_account,
    IFF(dim_billing_account_id_invoice_owner_account != dim_billing_account_id_creator_account, TRUE, FALSE)
      AS was_purchased_through_reseller,
    zuora_subscription.multi_year_deal_subscription_linkage,
    zuora_subscription.ramp_id,
    COALESCE (ramp_id != '' OR (multi_year_deal_subscription_linkage != '' AND multi_year_deal_subscription_linkage IS NOT NULL AND multi_year_deal_subscription_linkage != 'Not a ramp'), FALSE) AS is_ramp,

    --Date Information
    zuora_subscription.subscription_start_date,
    zuora_subscription.subscription_end_date,
    DATE_TRUNC('month', zuora_subscription.subscription_start_date::DATE)                                                                                                                         AS subscription_start_month,
    DATE_TRUNC('month', zuora_subscription.subscription_end_date::DATE)                                                                                                                           AS subscription_end_month,
    date_details.fiscal_year                                                                                                                                                                      AS subscription_end_fiscal_year,
    date_details.fiscal_quarter_name_fy                                                                                                                                                           AS subscription_end_fiscal_quarter_name_fy,
    zuora_subscription.term_start_date::DATE                                                                                                                                                      AS term_start_date,
    zuora_subscription.term_end_date::DATE                                                                                                                                                        AS term_end_date,
    DATE_TRUNC('month', zuora_subscription.term_start_date::DATE)                                                                                                                                 AS term_start_month,
    DATE_TRUNC('month', zuora_subscription.term_end_date::DATE)                                                                                                                                   AS term_end_month,
    term_start_date.fiscal_year                                                                                                                                                                   AS term_start_fiscal_year,
    term_end_date.fiscal_year                                                                                                                                                                     AS term_end_fiscal_year,
    COALESCE (term_start_date.fiscal_year = term_end_date.fiscal_year, FALSE)                                                                                                                     AS is_single_fiscal_year_term_subscription,
    CASE
      WHEN LOWER(zuora_subscription.subscription_status) = 'active' AND subscription_end_date > CURRENT_DATE
        THEN DATE_TRUNC('month', DATEADD('month', zuora_subscription.current_term, zuora_subscription.subscription_end_date::DATE))
    END                                                                                                                                                                                           AS second_active_renewal_month,
    zuora_subscription.cancelled_date,
    zuora_subscription.auto_renew_native_hist,
    zuora_subscription.auto_renew_customerdot_hist,
    zuora_subscription.turn_on_cloud_licensing,
    zuora_subscription.turn_on_operational_metrics,
    zuora_subscription.contract_operational_metrics,
    -- zuora_subscription.turn_on_usage_ping_required_metrics,
    NULL                                                                                                                                                                                          AS turn_on_usage_ping_required_metrics, -- https://gitlab.com/gitlab-data/analytics/-/issues/10172
    zuora_subscription.contract_auto_renewal,
    zuora_subscription.turn_on_auto_renewal,
    zuora_subscription.contract_seat_reconciliation,
    zuora_subscription.turn_on_seat_reconciliation,
    zuora_subscription.created_date                                                                                                                                                               AS subscription_created_datetime,
    zuora_subscription.updated_date                                                                                                                                                               AS subscription_updated_datetime,
    zuora_subscription.created_date::DATE                                                                                                                                                         AS subscription_created_date,
    zuora_subscription.updated_date::DATE                                                                                                                                                         AS subscription_updated_date,
    IFF(
      zuora_subscription.version = 1, 
      LEAST(zuora_subscription.created_date, zuora_subscription.subscription_start_date),
      zuora_subscription.created_date
      )                                                                                                                                                                                           AS subscription_created_datetime_adjusted,
    COALESCE(
      LEAD(subscription_created_datetime_adjusted)
        OVER (
          PARTITION BY zuora_subscription.subscription_name
          ORDER BY zuora_subscription.version
          ),
      GREATEST(
        CURRENT_DATE(),
        zuora_subscription.subscription_end_date
        ) 
      )                                                                                                                                                                                         AS next_subscription_created_datetime
  FROM zuora_subscription
  INNER JOIN zuora_account_source
    ON zuora_subscription.account_id = zuora_account_source.account_id
  LEFT JOIN zuora_account_source AS invoice_owner
    ON zuora_subscription.invoice_owner_id = invoice_owner.account_id
  LEFT JOIN zuora_account_source AS creator_account
    ON zuora_subscription.creator_account_id = creator_account.account_id
  LEFT JOIN map_merged_crm_account
    ON zuora_account_source.crm_id = map_merged_crm_account.sfdc_account_id
  LEFT JOIN date_details
    ON zuora_subscription.subscription_end_date::DATE = date_details.date_day
  LEFT JOIN date_details AS term_start_date
    ON zuora_subscription.term_start_date = term_start_date.date_day
  LEFT JOIN date_details AS term_end_date
    ON zuora_subscription.term_end_date = term_end_date.date_day
  LEFT JOIN prep_billing_account_user
    ON zuora_subscription.created_by_id = prep_billing_account_user.zuora_user_id
  LEFT JOIN sfdc_customer_subscription_source
    ON zuora_subscription.subscription_id = sfdc_customer_subscription_source.current_zuora_subscription_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@mdrussell",
    created_date="2021-01-07",
    updated_date="2024-11-05"
) }}
