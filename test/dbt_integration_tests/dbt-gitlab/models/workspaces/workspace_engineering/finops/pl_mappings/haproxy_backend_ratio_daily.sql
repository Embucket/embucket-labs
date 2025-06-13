WITH source AS (

    SELECT *
    FROM {{ ref('ha_proxy_metrics')}}

)

SELECT date_trunc('day', recorded_at) as date_day,
backend_category as backend_category,
sum(egress_gibibytes) as usage_in_gib,
RATIO_TO_REPORT(SUM(egress_gibibytes)) OVER (PARTITION BY date_day) AS percent_backend_ratio
FROM source
GROUP BY 1, 2
