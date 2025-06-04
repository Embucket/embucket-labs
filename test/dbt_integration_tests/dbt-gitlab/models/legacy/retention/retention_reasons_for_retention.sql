WITH raw_mrr_totals_levelled AS (

    SELECT *
    FROM {{ref('mart_arr')}}
    WHERE product_tier_name != 'Trueup'

), mrr_totals_levelled AS (

    SELECT 
        subscription_name,
        subscription_name_slugify,
        dim_crm_account_id                                                                                              AS sfdc_account_id,
        oldest_subscription_in_cohort                                                                                   AS oldest_subscription_in_cohort,
        subscription_lineage                                                                                            AS lineage,
        arr_month                                                                                                       AS mrr_month,
        subscription_cohort_month                                                                                       AS zuora_subscription_cohort_month,
        subscription_cohort_quarter                                                                                     AS zuora_subscription_cohort_quarter,
        months_since_subscription_cohort_start                                                                          AS months_since_zuora_subscription_cohort_start,
        quarters_since_subscription_cohort_start                                                                        AS quarters_since_zuora_subscription_cohort_start,

        ARRAY_AGG(DISTINCT product_tier_name) WITHIN GROUP (ORDER BY product_tier_name ASC)                             AS original_product_category,
        ARRAY_AGG(DISTINCT product_delivery_type) WITHIN GROUP (ORDER BY product_delivery_type ASC)                     AS original_delivery,
        ARRAY_AGG(DISTINCT unit_of_measure) WITHIN GROUP (ORDER BY unit_of_measure ASC)                                 AS original_unit_of_measure,
        
        MAX(DECODE(product_tier_name,   --Need to account for the 'other' categories
        'Bronze', 1,
        'Silver', 2,
        'Gold', 3,

        'Starter', 1,
        'Premium', 2,
        'Ultimate', 3,

        0
        ))                                                                                                              AS original_product_ranking,
        SUM(quantity)                                                                                                   AS original_quantity,
        SUM(mrr)                                                                                                        AS original_mrr
    FROM raw_mrr_totals_levelled
    {{ dbt_utils.group_by(n=10) }}

), list AS ( --get all the subscription + their lineage + the month we're looking for MRR for (12 month in the future)

    SELECT
        subscription_name_slugify       AS original_sub,
        c.value::VARCHAR                AS subscriptions_in_lineage,
        mrr_month                       AS original_mrr_month,
        DATEADD('year', 1, mrr_month)   AS retention_month
    FROM mrr_totals_levelled,
    LATERAL FLATTEN(input =>SPLIT(lineage, ',')) C
    {{ dbt_utils.group_by(n=4) }}

), retention_subs AS ( --find which of those subscriptions are real and group them by their sub you're comparing to.

    SELECT   
        list.original_sub,
        list.retention_month,
        list.original_mrr_month,
        mrr_totals_levelled.original_product_category      AS retention_product_category,
        mrr_totals_levelled.original_delivery              AS retention_delivery,
        mrr_totals_levelled.original_quantity              AS retention_quantity,
        mrr_totals_levelled.original_unit_of_measure       AS retention_unit_of_measure,
        mrr_totals_levelled.original_product_ranking       AS retention_product_ranking,
        COALESCE(SUM(mrr_totals_levelled.original_mrr), 0) AS retention_mrr
    FROM list
    INNER JOIN mrr_totals_levelled
       ON retention_month = mrr_month
       AND subscriptions_in_lineage = subscription_name_slugify
    {{ dbt_utils.group_by(n=8) }}

), expansion AS (

    SELECT
        mrr_totals_levelled.*,
        retention_subs.*,
        COALESCE(retention_subs.retention_mrr, 0) AS net_retention_mrr,
        {{ retention_type('original_mrr', 'net_retention_mrr') }},
        {{ retention_reason('original_mrr', 'original_product_category', 'original_product_ranking',
                            'original_quantity', 'net_retention_mrr', 'retention_product_category', 
                            'retention_product_ranking', 'retention_quantity') }},
        {{ plan_change('original_product_ranking', 'original_mrr',
                       'retention_product_ranking', 'net_retention_mrr') }},
        {{ seat_change('original_quantity', 'original_unit_of_measure', 'original_mrr',
                       'retention_quantity', 'retention_unit_of_measure', 'net_retention_mrr') }},
        {{ monthly_price_per_seat_change('original_mrr', 'original_quantity', 'original_unit_of_measure',
                                         'net_retention_mrr', 'retention_quantity', 'retention_unit_of_measure') }}

    FROM mrr_totals_levelled
    LEFT JOIN retention_subs
        ON subscription_name_slugify = original_sub
        AND retention_subs.original_mrr_month = mrr_totals_levelled.mrr_month
    WHERE retention_mrr > original_mrr

), churn AS (

    SELECT
        mrr_totals_levelled.*,
        retention_subs.*,
        COALESCE(retention_subs.retention_mrr, 0)               AS net_retention_mrr,
        {{ retention_type('original_mrr', 'net_retention_mrr') }},
        {{ retention_reason('original_mrr', 'original_product_category', 'original_product_ranking',
                            'original_quantity', 'net_retention_mrr', 'retention_product_category', 
                            'retention_product_ranking', 'retention_quantity') }},
        {{ plan_change('original_product_ranking', 'original_mrr',
                       'retention_product_ranking', 'net_retention_mrr') }},
        {{ seat_change('original_quantity', 'original_unit_of_measure', 'original_mrr',
                       'retention_quantity', 'retention_unit_of_measure', 'net_retention_mrr') }},
        {{ monthly_price_per_seat_change('original_mrr', 'original_quantity', 'original_unit_of_measure',
                                         'net_retention_mrr', 'retention_quantity', 'retention_unit_of_measure') }}

    FROM mrr_totals_levelled
    LEFT JOIN retention_subs
        ON subscription_name_slugify = original_sub
        AND retention_subs.original_mrr_month = mrr_totals_levelled.mrr_month
    WHERE net_retention_mrr < original_mrr

), joined AS (

    SELECT
        subscription_name              AS zuora_subscription_name,
        oldest_subscription_in_cohort  AS zuora_subscription_id,
        DATEADD('year', 1, mrr_month)  AS retention_month, --THIS IS THE RETENTION MONTH, NOT THE MRR MONTH!!
        retention_type,
        retention_reason,
        plan_change,
        seat_change,
        monthly_price_per_seat_change,
        original_product_category,
        retention_product_category,
        original_delivery,
        retention_delivery,
        original_quantity,
        retention_quantity,
        original_unit_of_measure,
        retention_unit_of_measure,
        original_mrr,
        net_retention_mrr AS retention_mrr
    FROM expansion

    UNION ALL

    SELECT 
        subscription_name                                             AS zuora_subscription_name,
        oldest_subscription_in_cohort                                 AS zuora_subscription_id,
        DATEADD('year', 1, mrr_month)                                 AS retention_month, --THIS IS THE RETENTION MONTH, NOT THE MRR MONTH!!
        retention_type,
        retention_reason,
        plan_change,
        seat_change,
        monthly_price_per_seat_change,
        original_product_category,
        retention_product_category,
        original_delivery,
        retention_delivery,
        original_quantity,
        retention_quantity,
        original_unit_of_measure,
        retention_unit_of_measure,
        original_mrr,
        net_retention_mrr                                             AS retention_mrr
    FROM churn

)

SELECT
    joined.*,
    RANK() OVER(PARTITION by zuora_subscription_id, retention_type
        ORDER BY retention_month ASC)   AS rank_retention_type_month
FROM joined
WHERE retention_month <= DATEADD(month, -1, CURRENT_DATE)
