{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

-- PREPARATION STEPS
-- subscription data preparation

WITH sub_version AS (

-- marking the last version with row number

  SELECT
    dim_subscription.dim_subscription_id,
    dim_subscription.dim_crm_opportunity_id,
    dim_subscription.subscription_name,
    dim_subscription.subscription_start_date,
    dim_subscription.subscription_end_date,
    dim_subscription.dim_crm_account_id,
    dim_subscription.subscription_version,
    dim_subscription.zuora_renewal_subscription_name,
    ROW_NUMBER() OVER (PARTITION BY dim_subscription.subscription_name ORDER BY dim_subscription.subscription_version DESC) AS row_num,
    dim_billing_account.crm_entity,
    dim_billing_account.dim_billing_account_id
  FROM {{ ref('dim_subscription') }}
  LEFT JOIN {{ ref('dim_billing_account') }} 
    ON dim_subscription.dim_billing_account_id = dim_billing_account.dim_billing_account_id

),

last_sub_version AS (

-- picking the last version by the row number

  SELECT *
  FROM sub_version
  WHERE row_num = 1

),

cancelled_sub AS (

-- in last versions determine subscriptions cancelled per start date

  SELECT last_sub_version.*
  FROM last_sub_version
  WHERE last_sub_version.subscription_start_date = last_sub_version.subscription_end_date

),

first_version_non_cancelled AS (

-- joining to remove subscriptions cancelled per start date and filtering version 1 only for renewal subscriptions

  SELECT
    sub_version.dim_subscription_id,
    sub_version.dim_crm_opportunity_id,
    sub_version.subscription_name,
    sub_version.subscription_start_date,
    sub_version.subscription_end_date,
    sub_version.dim_crm_account_id,
    sub_version.subscription_version,
    sub_version.row_num,
    sub_version.crm_entity,
    sub_version.dim_billing_account_id
  FROM sub_version
  LEFT JOIN cancelled_sub 
    ON sub_version.subscription_name = cancelled_sub.subscription_name
  WHERE cancelled_sub.subscription_name IS NULL
    AND sub_version.subscription_version = 1

),

last_version_non_cancelled AS (

--joining to remove cancelled subscriptions for last version for prior subscriptions in lineage

  SELECT
    sub_version.dim_subscription_id,
    sub_version.dim_crm_opportunity_id,
    sub_version.subscription_name,
    sub_version.subscription_start_date,
    sub_version.subscription_end_date,
    sub_version.dim_crm_account_id,
    sub_version.subscription_version,
    sub_version.zuora_renewal_subscription_name,
    sub_version.row_num,
    sub_version.crm_entity,
    mart_crm_opportunity.record_type_name
  FROM sub_version
  LEFT JOIN cancelled_sub 
    ON sub_version.subscription_name = cancelled_sub.subscription_name
  LEFT JOIN {{ ref('mart_crm_opportunity') }} AS mart_crm_opportunity
    ON sub_version.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
  WHERE cancelled_sub.subscription_name IS NULL
    AND sub_version.row_num = 1
    AND mart_crm_opportunity.record_type_name = 'Standard'

),

-- PART 1
-- renewal opportunity linking

opportunity_basis_renewal AS (

-- opportunity basis data for renewals where opportunity is renewal and quote is new business, e.g. late renewal

  SELECT
    mart_crm_opportunity.dim_crm_opportunity_id,
    mart_crm_opportunity.close_month,
    mart_crm_opportunity.subscription_type,
    dim_quote.subscription_action_type,
    first_version_non_cancelled.dim_subscription_id AS subscription_id,
    first_version_non_cancelled.subscription_name,
    first_version_non_cancelled.subscription_start_date,
    first_version_non_cancelled.subscription_end_date,
    first_version_non_cancelled.dim_crm_account_id,
    first_version_non_cancelled.subscription_version,
    first_version_non_cancelled.row_num
  FROM {{ ref('mart_crm_opportunity') }} AS mart_crm_opportunity
  LEFT JOIN {{ ref('fct_quote') }} 
    ON mart_crm_opportunity.dim_crm_opportunity_id = fct_quote.dim_crm_opportunity_id
  LEFT JOIN {{ ref('dim_quote') }} 
    ON fct_quote.dim_quote_id = dim_quote.dim_quote_id
  LEFT JOIN first_version_non_cancelled 
    ON mart_crm_opportunity.dim_crm_opportunity_id = first_version_non_cancelled.dim_crm_opportunity_id
  WHERE mart_crm_opportunity.is_won = TRUE
    AND mart_crm_opportunity.is_web_portal_purchase = FALSE
    AND mart_crm_opportunity.subscription_type = 'Renewal'
    AND mart_crm_opportunity.record_type_name = 'Standard'
    AND dim_quote.subscription_action_type = 'New Subscription'
    AND dim_quote.is_primary_quote = TRUE
    AND mart_crm_opportunity.close_month >= DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_DATE()))
    AND subscription_id IS NOT NULL

),

renewal_subscription_linking AS (

-- final model for opportunities renewal and quotes new subscriptions with suggested prior subscriptions, use case e.g. late renewal, debook/rebook, contract reset

  SELECT
    opportunity_basis_renewal.close_month             AS renewal_close_month,
    opportunity_basis_renewal.dim_crm_opportunity_id  AS renewal_dim_crm_opportunity_id,
    opportunity_basis_renewal.subscription_name       AS renewal_subscription_name,
    opportunity_basis_renewal.subscription_start_date AS renewal_subscription_start_date,
    last_version_non_cancelled.dim_crm_account_id     AS previous_term_dim_crm_account_id,
    last_version_non_cancelled.subscription_name      AS previous_term_subscription_name,
    last_version_non_cancelled.dim_subscription_id    AS previous_term_subscription_id,
    last_version_non_cancelled.subscription_end_date  AS previous_term_subscription_end_date,
    last_version_non_cancelled.zuora_renewal_subscription_name,
    CASE
      WHEN renewal_close_month IS NOT NULL
        THEN 'renewal use case'
      ELSE 'n/a'
    END                                               AS use_case
  FROM opportunity_basis_renewal
  LEFT JOIN last_version_non_cancelled 
    ON opportunity_basis_renewal.dim_crm_account_id = last_version_non_cancelled.dim_crm_account_id
  WHERE (
    (opportunity_basis_renewal.subscription_start_date = last_version_non_cancelled.subscription_end_date)
    OR --late renewals within 6 months, beyond 6 months as per policy it's not a renewal any more
    (
      opportunity_basis_renewal.subscription_start_date > last_version_non_cancelled.subscription_end_date
      AND opportunity_basis_renewal.subscription_start_date < (last_version_non_cancelled.subscription_end_date + 180)
    )
    OR --use cases where the start date is less by a few days than the end date of the previous term subscription
    (
      opportunity_basis_renewal.subscription_start_date < last_version_non_cancelled.subscription_end_date
      AND opportunity_basis_renewal.subscription_start_date > (last_version_non_cancelled.subscription_end_date - 3)
    )
  )
  AND opportunity_basis_renewal.subscription_name != TRIM(last_version_non_cancelled.zuora_renewal_subscription_name)
  ORDER BY 3, 7

),

-- PART 2
-- new business opportunity linking

opportunity_basis_new_business AS (

-- opportunity basis data for renewals where opportunity is new business and quote is new business

  SELECT
    mart_crm_opportunity.dim_crm_opportunity_id,
    mart_crm_opportunity.close_month,
    mart_crm_opportunity.subscription_type,
    dim_quote.subscription_action_type,
    first_version_non_cancelled.dim_subscription_id AS subscription_id,
    first_version_non_cancelled.subscription_name,
    first_version_non_cancelled.subscription_start_date,
    first_version_non_cancelled.subscription_end_date,
    first_version_non_cancelled.dim_crm_account_id,
    first_version_non_cancelled.subscription_version,
    first_version_non_cancelled.row_num
  FROM {{ ref('mart_crm_opportunity') }} AS mart_crm_opportunity
  LEFT JOIN {{ ref('fct_quote') }} 
    ON mart_crm_opportunity.dim_crm_opportunity_id = fct_quote.dim_crm_opportunity_id
  LEFT JOIN {{ ref('dim_quote') }} 
    ON fct_quote.dim_quote_id = dim_quote.dim_quote_id
  LEFT JOIN first_version_non_cancelled 
    ON mart_crm_opportunity.dim_crm_opportunity_id = first_version_non_cancelled.dim_crm_opportunity_id
  WHERE mart_crm_opportunity.is_won = TRUE
    AND mart_crm_opportunity.is_web_portal_purchase = FALSE
    AND mart_crm_opportunity.subscription_type = 'New Business'
    AND mart_crm_opportunity.record_type_name = 'Standard'
    AND dim_quote.subscription_action_type = 'New Subscription'
    AND dim_quote.is_primary_quote = TRUE
    AND mart_crm_opportunity.close_month >= DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_DATE()))
    AND subscription_id IS NOT NULL

),

new_business AS (

-- final model for opportunities new business and quotes new subscriptions with suggested prior subscriptions, use case e.g. legacy ramp

  SELECT
    opportunity_basis_new_business.close_month             AS renewal_close_month,
    opportunity_basis_new_business.dim_crm_opportunity_id  AS renewal_dim_crm_opportunity_id,
    opportunity_basis_new_business.subscription_name       AS renewal_subscription_name,
    opportunity_basis_new_business.subscription_start_date AS renewal_subscription_start_date,
    last_version_non_cancelled.dim_crm_account_id          AS previous_term_dim_crm_account_id,
    last_version_non_cancelled.subscription_name           AS previous_term_subscription_name,
    last_version_non_cancelled.dim_subscription_id         AS previous_term_subscription_id,
    last_version_non_cancelled.subscription_end_date       AS previous_term_subscription_end_date,
    last_version_non_cancelled.zuora_renewal_subscription_name,
    CASE
      WHEN renewal_close_month IS NOT NULL
        THEN 'new business use case'
      ELSE 'n/a'
    END                                                    AS use_case
  FROM opportunity_basis_new_business
  LEFT JOIN last_version_non_cancelled 
    ON opportunity_basis_new_business.dim_crm_account_id = last_version_non_cancelled.dim_crm_account_id
  WHERE (
    (opportunity_basis_new_business.subscription_start_date = last_version_non_cancelled.subscription_end_date)
    OR
    (
      opportunity_basis_new_business.subscription_start_date > last_version_non_cancelled.subscription_end_date
      AND opportunity_basis_new_business.subscription_start_date < (last_version_non_cancelled.subscription_end_date + 1)
    )
    OR
    (
      opportunity_basis_new_business.subscription_start_date < last_version_non_cancelled.subscription_end_date
      AND opportunity_basis_new_business.subscription_start_date > (last_version_non_cancelled.subscription_end_date - 1)
    )
  )
  AND opportunity_basis_new_business.subscription_name != TRIM(last_version_non_cancelled.zuora_renewal_subscription_name)
  ORDER BY 3, 7

),

-- PART 3
-- change of entity subscription linking

opportunity_basis_entity AS (

--SFDC data new entity on the quote and existing subscription and its entity from Zuora

  SELECT
    mart_crm_opportunity.dim_crm_opportunity_id,
    mart_crm_opportunity.close_month,
    fct_quote.quote_exist_subscription_id            AS existing_subscription_id,
    dim_quote.quote_entity,
    sub_version.subscription_version                 AS renewal_subscription_version,
    sub_version.crm_entity                           AS existing_zuora_entity,
    sub_version.subscription_name,
    last_sub_version.zuora_renewal_subscription_name,
    sub_version.dim_crm_account_id,
    sub_version.subscription_start_date,
    sub_version.subscription_end_date,
    sub_version.subscription_version  AS previous_subscription_version,
    mart_crm_opportunity.subscription_type
  FROM {{ ref('mart_crm_opportunity') }} AS mart_crm_opportunity
  LEFT JOIN {{ ref('fct_quote') }} 
    ON mart_crm_opportunity.dim_crm_opportunity_id = fct_quote.dim_crm_opportunity_id
  LEFT JOIN {{ ref('dim_quote') }} 
    ON fct_quote.dim_quote_id = dim_quote.dim_quote_id
  LEFT JOIN sub_version 
    ON fct_quote.quote_exist_subscription_id = sub_version.dim_subscription_id
  LEFT JOIN last_sub_version 
    ON last_sub_version.subscription_name = sub_version.subscription_name
  WHERE mart_crm_opportunity.is_won = TRUE
    AND mart_crm_opportunity.subscription_type = 'Renewal'
    AND mart_crm_opportunity.close_month >= DATEADD(MONTH, -1, DATE_TRUNC(MONTH, CURRENT_DATE()))
    AND mart_crm_opportunity.record_type_name = 'Standard'
    AND dim_quote.quote_status = 'Sent to Z-Billing'
    AND fct_quote.quote_exist_subscription_id IS NOT NULL
    AND sub_version.crm_entity != dim_quote.quote_entity

),

entity AS (

  --joining the SFDC and Zuora data for the existing subscription where entity and account don't match with the new follow-up subscription

  SELECT
    opportunity_basis_entity.close_month                AS renewal_close_month,
    opportunity_basis_entity.dim_crm_opportunity_id     AS renewal_dim_crm_opportunity_id,
    last_sub_version_2.subscription_name                AS renewal_subscription_name,
    last_sub_version_2.subscription_start_date          AS renewal_subscription_start_date,
    last_sub_version_2.dim_crm_account_id               AS previous_term_dim_crm_account_id,
    opportunity_basis_entity.subscription_name          AS previous_term_subscription_name,
    last_sub_version_1.dim_subscription_id              AS previous_term_subscription_id,
    opportunity_basis_entity.subscription_end_date      AS previous_term_subscription_end_date,
    last_sub_version_1.zuora_renewal_subscription_name,
    CASE
      WHEN renewal_close_month IS NOT NULL
        THEN 'entity use case'
      ELSE 'n/a'
    END                                                 AS use_case
  FROM opportunity_basis_entity
  LEFT JOIN last_sub_version AS last_sub_version_1
    ON opportunity_basis_entity.subscription_name = last_sub_version_1.subscription_name 
  LEFT JOIN last_sub_version AS last_sub_version_2 
    ON opportunity_basis_entity.dim_crm_account_id = last_sub_version_2.dim_crm_account_id
  WHERE opportunity_basis_entity.subscription_end_date = last_sub_version_2.subscription_start_date
    AND opportunity_basis_entity.existing_zuora_entity != last_sub_version_2.crm_entity
    AND last_sub_version_2.subscription_name != TRIM(opportunity_basis_entity.zuora_renewal_subscription_name)

),

final AS (

  SELECT *
  FROM entity

  UNION

  SELECT *
  FROM renewal_subscription_linking

  UNION

  SELECT *
  FROM new_business

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-07-05",
updated_date="2024-11-08"
) }}
