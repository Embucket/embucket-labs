WITH zuora_accts AS (

    SELECT *
    FROM {{ ref('zuora_account') }}

),    zuora_subs AS (

    SELECT *
    FROM {{ ref('zuora_subscription') }}

), zuora_rp AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan') }}

), zuora_rpc AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge') }}

), zuora_i AS (

    SELECT *
    FROM {{ ref('zuora_invoice') }}
    WHERE status = 'Posted' -- posted is important!

), zuora_ii AS (

    SELECT *,
          date_trunc('month', service_start_date) :: DATE   AS service_month -- use current month
    FROM {{ ref('zuora_invoice_item') }}

), sub_months AS (

    SELECT
      country,
      account_number,
      cohort_month,
      cohort_quarter,
      subscription_name,
      subscription_name_slugify,
      oldest_subscription_in_cohort,
      lineage
    FROM {{ ref('zuora_base_mrr') }}
    {{ dbt_utils.group_by(n=8) }}

), charges as (

      SELECT
             s.subscription_name,
             s.subscription_name_slugify,
             ii.*
      FROM zuora_ii ii
             INNER JOIN zuora_i i ON i.invoice_id = ii.invoice_id
             LEFT JOIN zuora_rpc rpc ON rpc.rate_plan_charge_id = ii.rate_plan_charge_id
             LEFT JOIN zuora_rp rp ON rpc.rate_plan_id = rp.rate_plan_id
             LEFT JOIN zuora_subs s ON rp.subscription_id = s.subscription_id
  )

SELECT

  sub_months.*,
  charges.service_month,
  {{ dbt_utils.star(from=ref('zuora_invoice_item'), except=["SUBSCRIPTION_NAME", "SUBSCRIPTION_NAME_SLUGIFY"]) }}
FROM charges
LEFT JOIN sub_months
  ON charges.subscription_name_slugify = sub_months.subscription_name_slugify
WHERE sub_months.cohort_month IS NOT NULL
