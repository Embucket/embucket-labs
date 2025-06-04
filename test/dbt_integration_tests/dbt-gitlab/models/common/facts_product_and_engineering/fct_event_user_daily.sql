{{ config(
    materialized='view',
    tags=["mnpi_exception", "product"]
) }}

/*
Aggregate events by date, user, ultimate parent namespace, and event
Limit to 24 months of history for performance reasons
*/

WITH fct_event_user_daily AS (

  SELECT
    {{ dbt_utils.star(from=ref('fct_event_daily'), except=['IS_NULL_USER']) }}
  FROM {{ ref( 'fct_event_daily') }}
  WHERE is_null_user = FALSE
)


{{ dbt_audit(
    cte_ref="fct_event_user_daily",
    created_by="@iweeks",
    updated_by="@michellecooper",
    created_date="2022-04-09",
    updated_date="2024-10-10"
) }}
