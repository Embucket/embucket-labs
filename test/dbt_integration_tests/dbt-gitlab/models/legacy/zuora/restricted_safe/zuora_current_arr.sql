with subscription as (

  SELECT * FROM {{ref('zuora_subscription')}}

), account as (
  
  SELECT * FROM {{ref('zuora_account')}}

), rate_plan as (

  SELECT * FROM {{ref('zuora_rate_plan')}}

), rate_plan_charge as (

  SELECT * FROM {{ref('zuora_rate_plan_charge')}}

), arr AS
    (
      SELECT subscription.subscription_id,
             SUM(rate_plan_charge.mrr*12::NUMBER) AS current_arr
      FROM subscription
        JOIN account 
          ON subscription.account_id = account.account_id::VARCHAR
        JOIN rate_plan 
          ON rate_plan.subscription_id::VARCHAR = subscription.subscription_id
        JOIN rate_plan_charge 
          ON rate_plan_charge.rate_plan_id::VARCHAR = rate_plan.rate_plan_id::VARCHAR
      WHERE (subscription.subscription_status NOT IN ('Draft','Expired')) --DOUBLE CHECK THIS
      AND   rate_plan_charge.effective_start_date <= CURRENT_DATE
      AND   (rate_plan_charge.effective_end_date > CURRENT_DATE 
          OR rate_plan_charge.effective_end_date IS NULL)
      GROUP BY subscription.subscription_id
    )

SELECT SUM(CASE WHEN current_arr > 0 THEN 1 ELSE 0 END) AS over_0,
       SUM(CASE WHEN current_arr > 5000 THEN 1 ELSE 0 END) AS over_5k,
       SUM(CASE WHEN current_arr > 50000 THEN 1 ELSE 0 END) AS over_50k,
       SUM(CASE WHEN current_arr > 100000 THEN 1 ELSE 0 END) AS over_100k,
       SUM(current_arr) AS current_arr
FROM arr
