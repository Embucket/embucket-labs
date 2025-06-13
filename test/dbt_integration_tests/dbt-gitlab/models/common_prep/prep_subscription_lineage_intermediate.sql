with zuora_subscription as (

    SELECT *,
        row_number() OVER (
          PARTITION BY subscription_name, version
          ORDER BY updated_date DESC ) AS sub_row
    FROM {{ref('zuora_subscription')}}

     /*
      The partition deduplicates the subscriptions when there are
      more than one version at the same time.
      See account_id = '2c92a0fc55a0dc530155c01a026806bd' for
      an example.
     */

), zuora_subs_filtered AS (

  SELECT *  
  FROM zuora_subscription
  WHERE subscription_status IN ('Active', 'Cancelled')

), zuora_partitioned_filter AS(

  SELECT

    zuora_subs_filtered.*,
    -- Dates
    date_trunc('month', zuora_subs_filtered.subscription_start_date)::DATE                    AS subscription_start_month,
    date_trunc('month', dateadd('day', -1, zuora_subs_filtered.subscription_end_date))::DATE  AS subscription_end_month,
    date_trunc('month', zuora_subs_filtered.contract_effective_date)::DATE                    AS subscription_month,
    date_trunc('quarter', zuora_subs_filtered.contract_effective_date)::DATE                  AS subscription_quarter,
    date_trunc('year', zuora_subs_filtered.contract_effective_date)::DATE                     AS subscription_year
  
  FROM zuora_subs_filtered
  WHERE zuora_subs_filtered.sub_row = 1

), circular AS ( 
	
    -- Identify for, exclusion, subscriptions with circular references in renewals to prevent failure of zuora_subscription_lineage
  
  SELECT DISTINCT
    left_subs.subscription_id
  FROM zuora_partitioned_filter AS left_subs
  INNER JOIN zuora_partitioned_filter AS right_subs
    ON left_subs.zuora_renewal_subscription_name = right_subs.subscription_name
  WHERE left_subs.subscription_name = right_subs.zuora_renewal_subscription_name 

)

SELECT *
FROM zuora_partitioned_filter
WHERE subscription_id not in ( -- exclude circularly referenced subscriptions
                               SELECT subscription_id
                               FROM circular
                             )
