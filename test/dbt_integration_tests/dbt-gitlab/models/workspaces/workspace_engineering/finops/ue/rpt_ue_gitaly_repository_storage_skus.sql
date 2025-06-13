
{{ config(
    materialized='table',
    )
}}

WITH cost_data AS (

    SELECT * FROM {{ ref('rpt_gcp_billing_pl_day_ext')}}

)

SELECT date_day,
    gcp_project_id,
    gcp_service_description,
    gcp_sku_description,
    pricing_unit,
    pl_category,
    sum(usage_amount_in_pricing_units) as usage_amount_in_pricing_units,
    sum(net_cost) as net_cost
FROM cost_data
where level_2 = 'Repository Storage'
    and gcp_sku_description IN ('SSD backed PD Capacity', 'Storage PD Snapshot in US', 'Balanced PD Capacity', 'Storage PD Capacity')
    and date_day >= '2023-02-01'
group by 1,2,3,4,5,6
