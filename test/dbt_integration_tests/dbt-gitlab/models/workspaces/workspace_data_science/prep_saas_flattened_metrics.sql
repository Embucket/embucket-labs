{{
    config(
        materialized='incremental'
    )
}}

WITH dates AS (
  
		SELECT *
		FROM {{ ref('dim_date') }}
  
), saas_usage_ping AS (
  
		SELECT *
		FROM {{ ref('prep_saas_usage_ping_namespace') }}
		WHERE ping_date >= '2021-03-01'::DATE
			AND ping_name LIKE 'usage_activity_by_stage%'
			AND counter_value > 0 -- Filter out non-instances
			AND ping_date <= CURRENT_DATE -- Return data for complete month and current month
			{% if is_incremental() %}
			AND ping_date > (SELECT MAX(ping_date) FROM {{ this }})
			{% endif %}

), saas_last_monthly_ping_per_account AS (
  
		SELECT
			saas_usage_ping.dim_namespace_id,
			dates.first_day_of_month 					AS snapshot_month,
			saas_usage_ping.ping_date,
			saas_usage_ping.ping_name 				AS metrics_path,
			saas_usage_ping.counter_value     AS metrics_value
		FROM saas_usage_ping
		INNER JOIN dates
			ON saas_usage_ping.ping_date = dates.date_day
		QUALIFY ROW_NUMBER() OVER (
		PARTITION BY
			saas_usage_ping.dim_namespace_id,
			dates.first_day_of_month,
			saas_usage_ping.ping_name
		ORDER BY
			saas_usage_ping.ping_date DESC
		) = 1
  
), flattened_metrics AS (
  
		SELECT
			dim_namespace_id,
			snapshot_month,
			ping_date,
			metrics_path,
			metrics_value
		FROM saas_last_monthly_ping_per_account
  
)

SELECT *
FROM flattened_metrics
