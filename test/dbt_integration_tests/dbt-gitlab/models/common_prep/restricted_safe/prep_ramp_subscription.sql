-- SOURCES
-- Sources and data quality layer

{{ simple_cte([
    ('prep_crm_opportunity', 'prep_crm_opportunity'),
    ('prep_subscription', 'prep_subscription'),
    ('zuora_query_api_ramp_interval_source', 'zuora_query_api_ramp_interval_source')
]) }},

prep_subscription_prep AS (
    SELECT *
    FROM prep_subscription
    WHERE dim_subscription_id NOT IN ('2c92a0ff7a18802d017a1aad9e204660', '2c92a0ff77c3de750177cbbe988c5075', '2c92a0ff77c3de750177cbbe979b505f', '2c92a00772a809f90172ba12c5277eb6',  
    '8a128adf8d5f0b32018d62ba1f2b212e', 
    '8a129dce93490146019373987fb446e8', '2c92a00d735159840173596996fe4241',
    '2c92a00e72a2d6410172a573304542dd', '2c92a00d6ae928cd016b004e3f627668')

), 

prep_crm_opportunity_prep AS (

SELECT *
FROM prep_crm_opportunity
WHERE is_live = 1

),

-- PART 1
-- Model for all legacy ramps

-- Identify all cancelled subscriptions

cancelled_version AS (
    SELECT 
        subscription_name,
        subscription_version,
        (subscription_version - 1) AS previous_version,
        cancelled_date,
        subscription_end_date
    FROM prep_subscription_prep
    WHERE subscription_status = 'Cancelled'
), 

-- Identify subscriptions that were not cancelled per start date (e.g. contract resets)

all_cancelled AS (
    SELECT
        cancelled_version.subscription_name,
        cancelled_version.subscription_version,
        cancelled_version.previous_version,
        cancelled_version.cancelled_date,
        cancelled_version.subscription_end_date,
        prep_subscription_prep.subscription_end_date AS previous_subscription_end_date
    FROM cancelled_version
    LEFT JOIN prep_subscription_prep
        ON cancelled_version.subscription_name = prep_subscription_prep.subscription_name
        AND cancelled_version.previous_version = prep_subscription_prep.subscription_version
    WHERE cancelled_version.subscription_end_date != previous_subscription_end_date
),

-- Identify subscriptions created via CDot as ramp subscriptions are sales assisted

cdot_created_subscriptions AS (
    SELECT DISTINCT
        subscription_name
    FROM prep_subscription_prep
    WHERE subscription_version = 1
        AND created_by_id IN (
            '2c92a0107bde3653017bf00cd8a86d5a',
            '2c92a0fd55822b4d015593ac264767f2',
            '2c92a0fd567e2cff01568f5d58ec336f'
        )
),

-- Row number for subscription versions

sub_version AS (
    SELECT
        dim_subscription_id,
        dim_crm_opportunity_id,
        subscription_name,
        subscription_start_date,
        subscription_end_date,
        term_start_date,
        term_end_date,
        dim_crm_account_id,
        subscription_version,
        zuora_renewal_subscription_name,
        created_by_id,
        ROW_NUMBER() OVER (
            PARTITION BY subscription_name 
            ORDER BY subscription_version DESC
        ) AS row_num
    FROM prep_subscription_prep
),

-- Identify last subscription version

last_sub_version AS (
    SELECT *
    FROM sub_version
    WHERE row_num = 1
),

-- Identify subscriptions cancelled per start date

cancelled_per_start_date AS (
    SELECT last_sub_version.*
    FROM last_sub_version
    WHERE subscription_start_date = subscription_end_date
),

-- Identify subscriptions that were not cancelled per start date

first_version_non_cancelled AS (
    SELECT
        sub_version.dim_subscription_id,
        sub_version.dim_crm_opportunity_id,
        sub_version.subscription_name,
        sub_version.subscription_start_date,
        sub_version.subscription_end_date,
        sub_version.dim_crm_account_id,
        sub_version.subscription_version,
        sub_version.row_num
    FROM sub_version
    LEFT JOIN cancelled_per_start_date
        ON sub_version.subscription_name = cancelled_per_start_date.subscription_name
    WHERE cancelled_per_start_date.subscription_name IS NULL
        AND sub_version.subscription_version = 1
),

-- Identify last versions of subscriptions not cancelled per start date

last_version_non_cancelled AS (
    SELECT
        sub_version.dim_subscription_id,
        sub_version.dim_crm_opportunity_id,
        sub_version.subscription_name,
        sub_version.subscription_start_date,
        sub_version.subscription_end_date,
        sub_version.dim_crm_account_id,
        sub_version.subscription_version,
        sub_version.zuora_renewal_subscription_name,
        sub_version.row_num
    FROM sub_version
    LEFT JOIN cancelled_per_start_date
        ON sub_version.subscription_name = cancelled_per_start_date.subscription_name
    INNER JOIN prep_crm_opportunity_prep
        ON sub_version.dim_crm_opportunity_id = prep_crm_opportunity_prep.dim_crm_opportunity_id
        AND prep_crm_opportunity_prep.record_type_name = 'Standard'
    WHERE cancelled_per_start_date.subscription_name IS NULL
        AND sub_version.row_num = 1
),

-- Identify potential 1 year ramp subscription

sub_1 AS (
    SELECT
        prep_subscription_prep.subscription_name,
        TRIM(prep_subscription_prep.zuora_renewal_subscription_name) AS zuora_renewal_subscription_name,
        prep_subscription_prep.subscription_end_date,
        prep_subscription_prep.dim_crm_opportunity_id,
        prep_subscription_prep.subscription_created_date,
        prep_subscription_prep.dim_billing_account_id AS subscription_billing_account,
        prep_subscription_prep.dim_billing_account_id_invoice_owner_account AS invoice_owner
    FROM prep_subscription_prep
    LEFT JOIN all_cancelled
        ON prep_subscription_prep.subscription_name = all_cancelled.subscription_name
    WHERE prep_subscription_prep.zuora_renewal_subscription_name != ''
        AND all_cancelled.subscription_name IS NULL
),

-- Identify potential follow on ramp subscriptions

sub_2 AS (
    SELECT DISTINCT
        prep_subscription_prep.subscription_name,
        prep_subscription_prep.subscription_start_date,
        prep_subscription_prep.subscription_end_date,
        prep_subscription_prep.subscription_created_date,
        prep_subscription_prep.dim_billing_account_id AS renewal_subscription_billing_account,
        prep_subscription_prep.created_by_id,
        prep_subscription_prep.dim_billing_account_id_invoice_owner_account AS renewal_invoice_owner
    FROM prep_subscription_prep
    INNER JOIN first_version_non_cancelled
        ON prep_subscription_prep.subscription_name = first_version_non_cancelled.subscription_name
    LEFT JOIN cdot_created_subscriptions
        ON prep_subscription_prep.subscription_name = cdot_created_subscriptions.subscription_name
    WHERE cdot_created_subscriptions.subscription_name IS NULL
),

-- Populate the potential ramps and join based on zuora_renewal_subscription_name, start and end dates, billing account and invoice owner (this should exclude late renewal and change of entity use cases)

all_ramps_with_multiple_subs AS (
    SELECT DISTINCT
        sub_1.subscription_name,
        opp_1.opportunity_name,
        opp_1.opportunity_category,
        sub_1.subscription_created_date,
        sub_1.zuora_renewal_subscription_name,
        sub_1.subscription_end_date,
        sub_1.subscription_billing_account,
        opp_2.opportunity_name AS renewal_opportunity_name,
        opp_2.opportunity_category AS renewal_opportunity_category,
        sub_2.subscription_name AS sub_2_subscription_name,
        sub_2.subscription_start_date,
        sub_2.subscription_end_date AS renewal_subscription_end_date,
        sub_2.subscription_created_date AS renewal_subscription_created_date,
        sub_2.renewal_subscription_billing_account
    FROM sub_1
    INNER JOIN sub_2
        ON sub_1.zuora_renewal_subscription_name = sub_2.subscription_name
        AND sub_1.subscription_end_date = sub_2.subscription_start_date
        AND sub_1.subscription_billing_account = sub_2.renewal_subscription_billing_account
        AND sub_1.invoice_owner = sub_2.renewal_invoice_owner
    LEFT JOIN prep_crm_opportunity_prep opp_1
        ON sub_1.dim_crm_opportunity_id = opp_1.dim_crm_opportunity_id
    LEFT JOIN prep_subscription_prep
        ON prep_subscription_prep.subscription_name = sub_1.zuora_renewal_subscription_name
    LEFT JOIN prep_crm_opportunity_prep opp_2
        ON prep_subscription_prep.dim_crm_opportunity_id = opp_2.dim_crm_opportunity_id
),

-- Union all identified possible ramp subscriptions

unioned AS (
    SELECT 
        subscription_name,
        subscription_billing_account AS dim_billing_account_id,
        subscription_end_date
    FROM all_ramps_with_multiple_subs

    UNION

    SELECT
        sub_2_subscription_name AS subscription_name,
        renewal_subscription_billing_account AS dim_billing_account_id,
        renewal_subscription_end_date AS subscription_end_date
    FROM all_ramps_with_multiple_subs
),

-- Add end date for sorting and ramp interval order

all_subs AS (
    SELECT
        unioned.subscription_name,
        unioned.dim_billing_account_id,
        MIN(unioned.subscription_end_date) AS subscription_end_date
    FROM unioned
    LEFT JOIN prep_subscription_prep
        ON prep_subscription_prep.subscription_name = unioned.subscription_name
    GROUP BY 1, 2
),

-- Determine the ramp order

ramp_order AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY dim_billing_account_id 
            ORDER BY subscription_end_date ASC
        ) AS ramp_order
    FROM all_subs
),

-- Determine subscriptions where the last in lineage may be a standard renewal and not part of the ramp

non_ramp_renewal AS (
    SELECT 
        ramp_order.*,
        CASE 
            WHEN ramp_order.ramp_order = MAX(ramp_order.ramp_order) OVER (PARTITION BY ramp_order.dim_billing_account_id) 
            THEN TRUE 
            ELSE FALSE
        END AS is_last_in_lineage,
        prep_subscription_prep.dim_crm_opportunity_id,
        prep_crm_opportunity_prep.opportunity_category,
        prep_crm_opportunity_prep.subscription_type,
        prep_crm_opportunity_prep.opportunity_term
    FROM ramp_order
    LEFT JOIN prep_subscription_prep
        ON prep_subscription_prep.subscription_name = ramp_order.subscription_name
        AND prep_subscription_prep.subscription_version = 1
    LEFT JOIN prep_crm_opportunity_prep
        ON prep_crm_opportunity_prep.dim_crm_opportunity_id = prep_subscription_prep.dim_crm_opportunity_id
        
),

-- Determine if the last subscription in lineage was created with standard opportunity category

no_standard_last_sub AS (
    SELECT *
    FROM non_ramp_renewal
    WHERE is_last_in_lineage = FALSE
     OR (is_last_in_lineage = TRUE
        AND opportunity_category NOT ILIKE '%standard%')
            
),

-- Identify the count of subscriptions as part of a ramp

account_counts AS (
    SELECT
        dim_billing_account_id,
        COUNT(subscription_name) AS subscription_count
    FROM no_standard_last_sub
    GROUP BY dim_billing_account_id
    HAVING COUNT(subscription_name) > 1
),

-- Remove all subscription where count of intervals is 1

remove_where_one_subscription AS (
    SELECT 
        no_standard_last_sub.*,
        'legacy ramp' AS ramp_type
    FROM no_standard_last_sub
    INNER JOIN account_counts
        ON account_counts.dim_billing_account_id = no_standard_last_sub.dim_billing_account_id
),

-- Define the start of the ramp using Interval 1

ramp_1_start AS (
    SELECT 
        last_sub_version.term_start_date AS ramp_1_term_start_date,
        last_sub_version.term_end_date AS ramp_1_term_end_date,
        last_sub_version.dim_subscription_id,
        last_sub_version.dim_crm_opportunity_id,
        last_sub_version.subscription_start_date,
        last_sub_version.subscription_end_date AS sub_end_date,
        last_sub_version.dim_crm_account_id,
        last_sub_version.subscription_version,
        last_sub_version.zuora_renewal_subscription_name,
        last_sub_version.created_by_id,
        remove_where_one_subscription.*
    FROM remove_where_one_subscription
    LEFT JOIN last_sub_version
        ON remove_where_one_subscription.subscription_name = last_sub_version.subscription_name
    WHERE remove_where_one_subscription.ramp_order = 1
),

-- Identify all the subscription version for the first subscription for the ramp term

ramp_1_all_versions AS (
    SELECT
        ramp_1_start.dim_billing_account_id,
        ramp_1_start.dim_subscription_id,
        ramp_1_start.subscription_name,
        prep_subscription_prep.subscription_version,
        prep_subscription_prep.subscription_status,
        ramp_1_start.subscription_start_date,
        ramp_1_start.sub_end_date AS subscription_end_date,
        ramp_1_start.ramp_1_term_start_date AS ramp_interval_start_date,
        ramp_1_start.ramp_1_term_end_date AS ramp_interval_end_date,
        prep_subscription_prep.current_term,
        ramp_1_start.zuora_renewal_subscription_name,
        prep_subscription_prep.multi_year_deal_subscription_linkage,
        prep_subscription_prep.ramp_id,
        prep_subscription_prep.is_ramp,
        ramp_1_start.ramp_type,
        ramp_1_start.is_last_in_lineage,
        ramp_1_start.ramp_order,
        prep_subscription_prep.dim_crm_opportunity_id,
        ramp_1_start.opportunity_term,
        ramp_1_start.opportunity_category,
        ramp_1_start.subscription_type
    FROM ramp_1_start
    LEFT JOIN prep_subscription_prep
        ON prep_subscription_prep.subscription_name = ramp_1_start.subscription_name
        AND ramp_1_start.ramp_1_term_start_date = prep_subscription_prep.term_start_date
),

-- Define the end of the ramp using interval other than 1

ramp_non_1_end AS (
    SELECT 
        sub_version.term_start_date AS ramp_non_1_term_start_date,
        sub_version.term_end_date AS ramp_non_1_term_end_date,
        sub_version.dim_subscription_id,
        sub_version.dim_crm_opportunity_id,
        sub_version.subscription_start_date,
        sub_version.subscription_end_date AS sub_end_date,
        sub_version.dim_crm_account_id,
        sub_version.subscription_version,
        sub_version.zuora_renewal_subscription_name,
        sub_version.created_by_id,
        remove_where_one_subscription.*
    FROM remove_where_one_subscription
    LEFT JOIN sub_version
        ON remove_where_one_subscription.subscription_name = sub_version.subscription_name
    WHERE remove_where_one_subscription.ramp_order > 1
        AND sub_version.subscription_version = 1
),

-- Identify all the subscription version for the follow-on subscriptions for the ramp term

ramp_non_1_all_versions AS (
    SELECT
        ramp_non_1_end.dim_billing_account_id,
        ramp_non_1_end.dim_subscription_id,
        ramp_non_1_end.subscription_name,
        prep_subscription_prep.subscription_version,
        prep_subscription_prep.subscription_status,
        ramp_non_1_end.subscription_start_date,
        ramp_non_1_end.sub_end_date AS subscription_end_date,
        ramp_non_1_end.ramp_non_1_term_start_date AS ramp_interval_start_date,
        ramp_non_1_end.ramp_non_1_term_end_date AS ramp_interval_end_date,
        prep_subscription_prep.current_term,
        ramp_non_1_end.zuora_renewal_subscription_name,
        prep_subscription_prep.multi_year_deal_subscription_linkage,
        prep_subscription_prep.ramp_id,
        prep_subscription_prep.is_ramp,
        ramp_non_1_end.ramp_type,
        ramp_non_1_end.is_last_in_lineage,
        ramp_non_1_end.ramp_order,
        prep_subscription_prep.dim_crm_opportunity_id,
        ramp_non_1_end.opportunity_term,
        ramp_non_1_end.opportunity_category,
        ramp_non_1_end.subscription_type
    FROM ramp_non_1_end
    LEFT JOIN prep_subscription_prep
        ON prep_subscription_prep.subscription_name = ramp_non_1_end.subscription_name
        AND ramp_non_1_end.ramp_non_1_term_start_date = prep_subscription_prep.term_start_date
),

-- Union all the legacy ramp subscriptions with subscription version as the lowest grain

unioned_legacy_ramps AS (
    SELECT *
    FROM ramp_1_all_versions

    UNION

    SELECT *
    FROM ramp_non_1_all_versions
),

-- Identify the start and end date of the ramp

all_legacy_ramps_term AS (
    SELECT
        unioned_legacy_ramps.dim_billing_account_id,
        unioned_legacy_ramps.ramp_order,
        unioned_legacy_ramps.subscription_name,
        unioned_legacy_ramps.current_term,
        MIN(unioned_legacy_ramps.ramp_interval_start_date) OVER (
            PARTITION BY unioned_legacy_ramps.dim_billing_account_id
        ) AS ramp_start_date,
        MAX(unioned_legacy_ramps.ramp_interval_end_date) OVER (
            PARTITION BY unioned_legacy_ramps.dim_billing_account_id
        ) AS ramp_end_date,
        MAX(unioned_legacy_ramps.ramp_order) OVER (
            PARTITION BY unioned_legacy_ramps.dim_billing_account_id
        ) AS ramp_interval_count,
        ROW_NUMBER() OVER (
            PARTITION BY unioned_legacy_ramps.dim_billing_account_id 
            ORDER BY unioned_legacy_ramps.ramp_order
        ) AS one_entry_account,
        ROW_NUMBER() OVER (
            PARTITION BY unioned_legacy_ramps.subscription_name 
            ORDER BY unioned_legacy_ramps.subscription_version
        ) AS one_entry_subscription
    FROM unioned_legacy_ramps
),

-- Identify one entry per account

all_legacy_ramps_term_one_line_account AS (
    SELECT *
    FROM all_legacy_ramps_term
    WHERE one_entry_account = 1
),

-- Identify one entry per subscription

all_legacy_ramps_term_one_line_subscription AS (
    SELECT *
    FROM all_legacy_ramps_term
    WHERE one_entry_subscription = 1
),

-- Calculate the full ramp term

term_calculation AS (
    SELECT DISTINCT
        all_legacy_ramps_term_one_line_subscription.subscription_name,
        all_legacy_ramps_term_one_line_subscription.dim_billing_account_id,
        all_legacy_ramps_term_one_line_subscription.current_term,
        SUM(all_legacy_ramps_term_one_line_subscription.current_term) OVER (
            PARTITION BY all_legacy_ramps_term_one_line_subscription.dim_billing_account_id
        ) AS full_ramp_term
    FROM all_legacy_ramps_term_one_line_subscription
),

-- Final outcome for the legacy ramps

legacy_ramps_final AS (
    SELECT 
        unioned_legacy_ramps.dim_billing_account_id,
        unioned_legacy_ramps.dim_subscription_id,
        unioned_legacy_ramps.subscription_name,
        unioned_legacy_ramps.subscription_version,
        unioned_legacy_ramps.subscription_status,
        unioned_legacy_ramps.ramp_interval_start_date,
        unioned_legacy_ramps.ramp_interval_end_date,
        all_legacy_ramps_term_one_line_account.ramp_start_date,
        all_legacy_ramps_term_one_line_account.ramp_end_date,
        all_legacy_ramps_term_one_line_account.ramp_interval_count,
        unioned_legacy_ramps.current_term AS ramp_interval_term,
        term_calculation.full_ramp_term,
        unioned_legacy_ramps.zuora_renewal_subscription_name,
        unioned_legacy_ramps.multi_year_deal_subscription_linkage,
        unioned_legacy_ramps.ramp_id,
        unioned_legacy_ramps.is_ramp,
        unioned_legacy_ramps.ramp_type,
        unioned_legacy_ramps.is_last_in_lineage,
        unioned_legacy_ramps.ramp_order,
        unioned_legacy_ramps.dim_crm_opportunity_id,
        prep_crm_opportunity_prep.opportunity_term,
        prep_crm_opportunity_prep.opportunity_category,
        prep_crm_opportunity_prep.subscription_type
    FROM unioned_legacy_ramps
    LEFT JOIN all_legacy_ramps_term_one_line_account
        ON all_legacy_ramps_term_one_line_account.dim_billing_account_id = unioned_legacy_ramps.dim_billing_account_id
    LEFT JOIN prep_crm_opportunity_prep
        ON unioned_legacy_ramps.dim_crm_opportunity_id = prep_crm_opportunity_prep.dim_crm_opportunity_id
    LEFT JOIN term_calculation
        ON term_calculation.subscription_name = unioned_legacy_ramps.subscription_name
),

-- PART 2
-- Model for all new ramps

-- Identify ramp start and end dates

ramp_sorting AS (
    SELECT DISTINCT
        zuora_query_api_ramp_interval_source.ramp_id,
        MIN(zuora_query_api_ramp_interval_source.start_date) OVER (
            PARTITION BY zuora_query_api_ramp_interval_source.ramp_id
        ) AS ramp_start_date,
        MAX(zuora_query_api_ramp_interval_source.end_date) OVER (
            PARTITION BY zuora_query_api_ramp_interval_source.ramp_id
        ) AS ramp_end_date,
        MAX(REPLACE(zuora_query_api_ramp_interval_source.name, 'Interval ', '')::FLOAT) OVER (
            PARTITION BY zuora_query_api_ramp_interval_source.ramp_id
        ) AS ramp_interval_count
    FROM zuora_query_api_ramp_interval_source

), 

-- Final outcome for the new ramps

new_ramps_final AS (
    SELECT 
        prep_subscription_prep.dim_billing_account_id,
        prep_subscription_prep.dim_subscription_id,
        prep_subscription_prep.subscription_name,
        prep_subscription_prep.subscription_version,
        prep_subscription_prep.subscription_status,
        zuora_query_api_ramp_interval_source.start_date AS ramp_interval_start_date,
        zuora_query_api_ramp_interval_source.end_date AS ramp_interval_end_date,
        ramp_sorting.ramp_start_date,
        ramp_sorting.ramp_end_date,
        ramp_sorting.ramp_interval_count,
        DATEDIFF('month', zuora_query_api_ramp_interval_source.start_date, zuora_query_api_ramp_interval_source.end_date) AS ramp_interval_term,
        prep_subscription_prep.current_term AS full_ramp_term,
        prep_subscription_prep.zuora_renewal_subscription_name,
        prep_subscription_prep.multi_year_deal_subscription_linkage,
        prep_subscription_prep.ramp_id,
        prep_subscription_prep.is_ramp,
        'new ramp' AS ramp_type,
        TRUE AS is_last_in_lineage,
        REPLACE(zuora_query_api_ramp_interval_source.name, 'Interval ', '')::FLOAT AS ramp_order,
        prep_subscription_prep.dim_crm_opportunity_id,
        prep_crm_opportunity_prep.opportunity_term,
        prep_crm_opportunity_prep.opportunity_category,
        prep_crm_opportunity_prep.subscription_type
    FROM prep_subscription_prep
    LEFT JOIN prep_crm_opportunity_prep
        ON prep_subscription_prep.dim_crm_opportunity_id = prep_crm_opportunity_prep.dim_crm_opportunity_id
    LEFT JOIN zuora_query_api_ramp_interval_source
        ON zuora_query_api_ramp_interval_source.ramp_id = prep_subscription_prep.ramp_id
    LEFT JOIN ramp_sorting
        ON ramp_sorting.ramp_id = prep_subscription_prep.ramp_id
    WHERE prep_subscription_prep.ramp_id != ''
),

-- Union the legacy and new ramp tables

final AS (

SELECT *
FROM legacy_ramps_final

UNION

SELECT *
FROM new_ramps_final
)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2025-01-21",
updated_date="2025-01-21"
) }}