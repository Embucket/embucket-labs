WITH cost_data AS (

    SELECT * FROM {{ ref ('rpt_gcp_billing_pl_day_ext')}}

)

SELECT date_day,
    gcp_project_id,
    gcp_service_description,
    gcp_sku_description,
    pricing_unit,
    pl_category,
    level_3,
    level_4,
    sum(usage_amount_in_pricing_units)*60 as usage_amount_in_pricing_units,
    sum(net_cost) as net_cost
FROM cost_data
where level_4 IN ('linux small ', 'linux medium', 'linux large', 'linux xlarge', 'linux 2xlarge', 'linux medium gpu', 'linux private internal')
and date_day >= '2023-02-01'
group by 1,2,3,4,5,6,7,8
order by 1 desc