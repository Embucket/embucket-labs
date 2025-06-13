WITH actuals_cogs_opex AS (

     SELECT *
     FROM {{ref('netsuite_actuals_cogs_opex')}}

), actuals_income AS (

     SELECT *
     FROM {{ref('netsuite_actuals_income')}}

), combined AS (

     SELECT *
     FROM actuals_cogs_opex

     UNION All

     SELECT *
     FROM actuals_income

)

SELECT *
FROM combined
