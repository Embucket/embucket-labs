{{ config({
    "materialized": "view"
    })
}}

{{ dbt_utils.union_relations(
    relations=[ref('sheetload_cert_customer_segmentation_dashboard_source'),
               ref('sheetload_cert_customer_segmentation_sql_source'),
               ref('sheetload_cert_product_geo_dashboard_source'),
               ref('sheetload_cert_product_geo_sql_source'),
               ref('sheetload_cert_product_geo_viewer_source'),
               ref('sheetload_cert_customer_segmentation_viewer_source'),
               ref('sheetload_cert_pricing_customer_discount_dashboard_source'),
               ref('sheetload_cert_pricing_customer_discount_viewer_source'),
               ref('sheetload_cert_product_adoption_dashboard_user_source'),
               ref('sheetload_cert_sales_funnel_dashboard_user_source'),
               ref('sheetload_cert_sales_funnel_dashboard_developer_source')
               ]
) }}
