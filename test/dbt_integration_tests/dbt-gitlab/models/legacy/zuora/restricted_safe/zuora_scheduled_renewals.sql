WITH zuora_base_mrr AS (

    SELECT *
    FROM {{ ref('zuora_base_mrr') }}

)

SELECT account_number,
        account_name,
        rate_plan_charge_name,
        rate_plan_charge_number,
        currency,
        effective_start_date,
        effective_end_date,
        subscription_start_date,
        exclude_from_renewal_report,
        mrr,
        mrr * 12 as arr
FROM zuora_base_mrr
WHERE subscription_status = 'Active'
AND EXCLUDE_FROM_RENEWAL_REPORT != 'Yes'
AND date_trunc('year', effective_end_date)::DATE >= date_trunc('year', CURRENT_DATE)::DATE