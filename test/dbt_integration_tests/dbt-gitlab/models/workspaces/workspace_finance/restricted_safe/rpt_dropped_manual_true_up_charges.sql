{{ config({
    "tags": ["true_up"]
    })
}}

WITH base_all_time AS (
  
  SELECT DISTINCT 
    subscription_name
  FROM {{ source('snapshots', 'mart_charge_snapshot') }}
  WHERE rate_plan_charge_name = 'manual true up allocation'

), base_live AS (

  SELECT DISTINCT 
    subscription_name
  FROM {{ ref('mart_charge') }}
  WHERE rate_plan_charge_name = 'manual true up allocation'

)

SELECT *
FROM base_all_time
WHERE subscription_name NOT IN (SELECT subscription_name FROM base_live)
-- Three subscriptions were incorrectly labeled as true-ups, so these exceptions need to be manually removed from the list of dropped true ups.
  AND subscription_name NOT IN ('A-S00069616', 'A-S00047777', 'A-S00052622')